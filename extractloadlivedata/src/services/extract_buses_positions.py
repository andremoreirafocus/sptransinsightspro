import requests
import time
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Tuple
from src.infra.structured_logging import get_structured_logger
from src.logging_taxonomy import ALLOWED_EVENTS, ALLOWED_STATUSES, LogStatus
from src.services.exceptions import PositionsDownloadError

structured_logger = get_structured_logger(
    service="extractloadlivedata",
    component="extract_buses_positions",
    logger_name=__name__,
    allowed_events=ALLOWED_EVENTS,
    allowed_statuses=ALLOWED_STATUSES,
)
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
        if buses_positions_response_is_valid(buses_positions_payload):
            if retries > 0:
                structured_logger.info(
                    event="extract_positions_succeeded",
                    status=LogStatus.SUCCEEDED,
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
                status=LogStatus.FAILED,
                message="Max retries reached. Download failed. Skipping this extraction cycle.",
            )
            error = PositionsDownloadError(
                "max retries reached while downloading positions"
            )
            setattr(error, "retries", retries)
            raise error
        structured_logger.warning(
            event="extract_positions_failed",
            status=LogStatus.RETRY,
            message=f"Invalid buses positions response structure! Retrying in {back_off} seconds...",
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
        status=LogStatus.SUCCEEDED,
        message=f"[{datetime.now().strftime('%H:%M:%S')}] Ref SPTrans: {reference_time} | Veículos Ativos: {total_vehicles}",
    )
    return buses_positions, reference_time


def extract_buses_positions(
    base_url: str, token: str, session: Optional[Any] = None
) -> Optional[PayloadDict]:
    session = session or requests.Session()
    auth_url = f"{base_url}/Login/Autenticar?token={token}"
    try:
        response_auth = session.post(auth_url, timeout=DEFAULT_API_TIMEOUT_SECONDS)
        if response_auth.status_code == 200 and response_auth.text.lower() == "true":
            structured_logger.info(
                event="extract_positions_started",
                status=LogStatus.STARTED,
                message=f"[{datetime.now().strftime('%H:%M:%S')}] Succesfully authenticated!",
            )
        else:
            structured_logger.error(
                event="extract_positions_failed",
                status=LogStatus.FAILED,
                message="Authentication error. Verify your Token.",
            )
            structured_logger.error(
                event="extract_positions_failed",
                status=LogStatus.FAILED,
                message=f"{response_auth.status_code} {response_auth.text}",
            )
            return
    except Exception as e:
        structured_logger.error(
            event="extract_positions_failed",
            status=LogStatus.FAILED,
            message=f"Error connecting: {e}",
        )
        return None
    try:
        posicao_url = f"{base_url}/Posicao"
        structured_logger.info(
            event="extract_positions_started",
            status=LogStatus.STARTED,
            message=f"[{datetime.now().strftime('%H:%M:%S')}] Get posicao started!",
        )
        response = session.get(posicao_url, timeout=DEFAULT_API_TIMEOUT_SECONDS)
        if response.status_code == 200:
            structured_logger.info(
                event="extract_positions_succeeded",
                status=LogStatus.SUCCEEDED,
                message=f"[{datetime.now().strftime('%H:%M:%S')}] Get posicao status OK!",
            )
            data = response.json()
            return data
        else:
            structured_logger.error(
                event="extract_positions_failed",
                status=LogStatus.FAILED,
                message=f"Error getting positions: {response.status_code}",
            )
    except Exception as e:
        structured_logger.error(
            event="extract_positions_failed",
            status=LogStatus.FAILED,
            message=f"Error during execution: {e}",
        )
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
            status=LogStatus.FAILED,
            message="Payload does not have a valid structure.",
        )
        structured_logger.error(
            event="extract_positions_failed",
            status=LogStatus.FAILED,
            message=f"Payload content: {buses_positions}",
        )
        return False
    required_fields = ["hr", "l"]
    for field in required_fields:
        if field not in buses_positions:
            structured_logger.error(
                event="extract_positions_failed",
                status=LogStatus.FAILED,
                message=f"Missing required payload field: {field}",
            )
            return False
    return True


def get_buses_positions_summary(buses_positions: Any) -> Tuple[str, Any]:
    if not isinstance(buses_positions, dict):
        structured_logger.error(
            event="extract_positions_failed",
            status=LogStatus.FAILED,
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
            status=LogStatus.FAILED,
            message=f"Error processing positions summary: {e}",
        )
        return "NaN", "NaN"
