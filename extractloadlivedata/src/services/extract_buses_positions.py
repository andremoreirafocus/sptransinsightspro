from src.infra.structured_logging import get_structured_logger
from src.domain.events import EVENT_STATUS_FAILED, EVENT_STATUS_RETRY, EVENT_STATUS_STARTED, EVENT_STATUS_SUCCEEDED
from src.services.exceptions import PositionsDownloadError
import requests  # type: ignore[import-untyped]
import time
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Tuple
import re
from html import unescape


structured_logger = get_structured_logger(
    service="extractloadlivedata",
    component="extract_buses_positions",
    logger_name=__name__,)
DEFAULT_API_TIMEOUT_SECONDS = 10
ConfigDict = Dict[str, Any]
PayloadDict = Dict[str, Any]
DownloadResult = Dict[str, Any]


def extract_buses_positions_with_retries(
    config: ConfigDict,
    session: Optional[Any] = None,
    sleep_fn: Optional[Callable[[int], None]] = None,
    with_metrics: bool = False,
) -> Any:
    def get_config(config):
        token = config["TOKEN"]
        api_base_url = config["API_BASE_URL"]
        api_max_retries = int(config["API_MAX_RETRIES"])

        return api_max_retries, token, api_base_url

    api_max_retries, token, api_base_url = get_config(config)

    sleep_fn = sleep_fn or time.sleep
    back_off = 1
    for retries in range(api_max_retries + 1):
        buses_positions_payload = extract_buses_positions(
            token=token,
            base_url=api_base_url,
            session=session,
        )
        structured_logger.info(
            event="extract_positions_succeeded",
            status=EVENT_STATUS_SUCCEEDED,
            message=f"valor de buses_positions_payload: {buses_positions_payload}",
        )
        if buses_positions_payload is not None:
            if buses_positions_response_is_valid(buses_positions_payload):
                if retries > 0:
                    structured_logger.info(
                        event="extract_positions_succeeded",
                        status=EVENT_STATUS_SUCCEEDED,
                        message=f"Download successful after {retries} {'retry' if retries == 1 else 'retries'}.",
                    )
                if with_metrics:
                    return {
                        "result": buses_positions_payload,
                        "metrics": {
                            "retries": retries,
                        },
                    }
                return buses_positions_payload
        if retries >= api_max_retries:
            structured_logger.error(
                event="extract_positions_failed",
                status=EVENT_STATUS_FAILED,
                message="Max retries reached. Download failed. Skipping this extraction cycle.",
            )
            error = PositionsDownloadError(
                "max retries reached while downloading positions"
            )
            setattr(error, "retries", retries)
            raise error
        structured_logger.warning(
            event="extract_positions_failed",
            status=EVENT_STATUS_RETRY,
            message=f"Extraction of buses positions did not succeed. Retrying in {back_off} seconds...",
        )
        sleep_fn(back_off)
        back_off *= 2


def get_buses_positions_with_metadata(
    buses_positions_payload: PayloadDict,
) -> Tuple[PayloadDict, str]:
    reference_time, total_vehicles = get_buses_positions_summary(
        buses_positions_payload
    )
    buses_positions = {
        "metadata": {
            "extracted_at": datetime.now().isoformat(),
            "source": "sptrans_api_v2",
            "total_vehicles": total_vehicles,
        },
        "payload": buses_positions_payload,
    }
    structured_logger.info(
        event="extract_positions_succeeded",
        status=EVENT_STATUS_SUCCEEDED,
        message=f"[{datetime.now().strftime('%H:%M:%S')}] Ref SPTrans: {reference_time} | Veículos Ativos: {total_vehicles}",
    )
    return buses_positions, reference_time


def extract_buses_positions(
    base_url: str, token: str, session: Optional[Any] = None
) -> Optional[PayloadDict]:
    def sanitize_html_text(raw: str, max_len: int = 100) -> str:
        _TAG_RE = re.compile(r"<[^>]+>")
        if not raw:
            return ""
        no_tags = _TAG_RE.sub(" ", raw)
        normalized = " ".join(unescape(no_tags).split())
        return normalized[:max_len]

    session = session or requests.Session()
    auth_url = f"{base_url}/Login/Autenticar?token={token}"
    posicao_url = f"{base_url}/Posicao"
    try:
        response_auth = session.post(auth_url, timeout=DEFAULT_API_TIMEOUT_SECONDS)
        if response_auth.status_code == 200 and response_auth.text.lower() == "true":
            structured_logger.info(
                event="extract_positions_started",
                status=EVENT_STATUS_STARTED,
                message=f"[{datetime.now().strftime('%H:%M:%S')}] Succesfully authenticated!",
            )
        else:
            structured_logger.error(
                event="extract_positions_failed",
                status=EVENT_STATUS_FAILED,
                message=f"Error during authentication. {sanitize_html_text(response_auth.text)}",
            )
            return None
    except Exception as e:
        structured_logger.error(
            event="extract_positions_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Error connecting: {str(e)}",
        )
        return None
    try:
        structured_logger.info(
            event="extract_positions_started",
            status=EVENT_STATUS_STARTED,
            message=f"[{datetime.now().strftime('%H:%M:%S')}] Get posicao started!",
        )
        response = session.get(posicao_url, timeout=DEFAULT_API_TIMEOUT_SECONDS)
        if response.status_code == 200:
            structured_logger.info(
                event="extract_positions_succeeded",
                status=EVENT_STATUS_SUCCEEDED,
                message=f"[{datetime.now().strftime('%H:%M:%S')}] Get posicao status OK!",
            )
            data = response.json()
            return data
        else:
            structured_logger.error(
                event="extract_positions_failed",
                status=EVENT_STATUS_FAILED,
                message=f"Error {response.status_code} getting positions from {posicao_url}: {sanitize_html_text(response.text)}",
            )
            return None
    except Exception as e:
        structured_logger.error(
            event="extract_positions_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Error during execution: {str(e)}",
        )
        return None
    return None


def buses_positions_response_is_valid(buses_positions: Any) -> bool:
    """
    Validate the structure of the incoming buses_positions.
    :param buses_positions: The buses_positions to validate
    :return: True if valid, False otherwise
    """
    if not isinstance(buses_positions, dict):
        structured_logger.error(
            event="extract_positions_failed",
            status=EVENT_STATUS_FAILED,
            message="Payload does not have a valid structure.",
        )
        structured_logger.error(
            event="extract_positions_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Payload content: {buses_positions}",
        )
        return False
    required_fields = ["hr", "l"]
    for field in required_fields:
        if field not in buses_positions:
            structured_logger.error(
                event="extract_positions_failed",
                status=EVENT_STATUS_FAILED,
                message=f"Missing required payload field: {field}",
            )
            return False
    return True


def get_buses_positions_summary(buses_positions: Any) -> Tuple[str, Any]:
    if not isinstance(buses_positions, dict):
        structured_logger.error(
            event="extract_positions_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Incorrect data type: {type(buses_positions)}",
        )
        return "NaN", "NaN"
    try:
        reference_time = buses_positions.get("hr", "NaN")
        lines = buses_positions.get("l", [])  # 'l' contém a lista de lines e veículos
        total_vehicles = sum([len(line.get("vs", [])) for line in lines])
        return reference_time, total_vehicles
    except Exception as e:
        structured_logger.error(
            event="extract_positions_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Error processing positions summary: {e}",
        )
        return "NaN", "NaN"


