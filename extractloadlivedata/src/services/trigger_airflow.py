import requests  # type: ignore[import-untyped]
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.infra.cache import add_to_cache, get_from_cache, remove_from_cache
from src.infra.structured_logging import get_structured_logger
from src.domain.events import EVENT_STATUS_FAILED, EVENT_STATUS_SKIPPED, EVENT_STATUS_STARTED, EVENT_STATUS_SUCCEEDED
from src.services.exceptions import IngestNotificationError

structured_logger = get_structured_logger(
    service="extractloadlivedata",
    component="trigger_airflow",
    logger_name=__name__,)

ConfigDict = Dict[str, Any]
AuthTuple = Tuple[str, str]
PendingInvocationMetrics = Dict[str, int]
PendingInvocationResult = Dict[str, Any]


def create_pending_invokation(
    config: ConfigDict,
    filename: str,
    cache_factory: Optional[Callable[..., Any]] = None,
) -> None:
    """Add a pending invocation to the cache."""

    def get_config(config):
        cache_dir = config["INVOKATIONS_CACHE_DIR"]
        return cache_dir

    structured_logger.info(
        event="pending_storage_file_started",
        status=EVENT_STATUS_STARTED,
        message=f"Creating pending invokation for file '{filename}'",
    )
    # Use filename as key, storing the filename as value
    marker_name = f"{filename.split('.')[0]}.pending"
    cache_dir = get_config(config)
    add_to_cache(cache_dir, marker_name, filename, cache_factory=cache_factory)

    structured_logger.info(
        event="pending_storage_file_succeeded",
        status=EVENT_STATUS_SUCCEEDED,
        message=f"Pending invokation created in cache with key '{marker_name}' and value '{filename}'",
    )


def remove_pending_invokation(
    config: ConfigDict,
    marker_name: str,
    cache_factory: Optional[Callable[..., Any]] = None,
) -> None:
    """Remove a pending invocation from the cache."""

    def get_config(config):
        cache_dir = config["INVOKATIONS_CACHE_DIR"]
        return cache_dir

    structured_logger.info(
        event="pending_storage_file_started",
        status=EVENT_STATUS_STARTED,
        message=f"Removing pending invokation marker '{marker_name}'",
    )
    cache_dir = get_config(config)
    remove_from_cache(cache_dir, marker_name, cache_factory=cache_factory)


def get_pending_invokations(
    config: ConfigDict,
    cache_factory: Optional[Callable[..., Any]] = None,
) -> List[Any]:
    """Retrieve all pending invocations from the cache."""

    def get_config(config):
        cache_dir = config["INVOKATIONS_CACHE_DIR"]
        return cache_dir

    structured_logger.info(
        event="pending_storage_scan_succeeded",
        status=EVENT_STATUS_STARTED,
        message="Checking for pending invokations...",
    )
    cache_dir = get_config(config)
    pending_markers = get_from_cache(cache_dir, cache_factory=cache_factory)
    structured_logger.info(
        event="pending_storage_scan_succeeded",
        status=EVENT_STATUS_SUCCEEDED,
        message=f"Found {len(pending_markers)} pending invokation(s).",
    )
    return pending_markers


def trigger_pending_airflow_dag_invokations(
    config: ConfigDict,
    post_fn: Optional[Callable[..., Any]] = None,
    cache_factory: Optional[Callable[..., Any]] = None,
    with_metrics: bool = False,
) -> Optional[PendingInvocationResult]:
    """Trigger all pending invocations and remove them if successful."""
    pending_markers = get_pending_invokations(config, cache_factory=cache_factory)
    success_count = 0
    failure_count = 0
    if pending_markers:
        for pending_marker in pending_markers:
            structured_logger.info(
                event="pending_storage_detected",
                status=EVENT_STATUS_STARTED,
                message=f"Pending invokation found: {pending_marker}",
            )
            structured_logger.info(
                event="pending_storage_detected",
                status=EVENT_STATUS_STARTED,
                message=f"Found {len(pending_markers)} pending invokation(s). Processing...",
            )
            try:
                if trigger_airflow_dag_run(config, pending_marker, post_fn=post_fn):
                    remove_pending_invokation(
                        config, pending_marker, cache_factory=cache_factory
                    )
                    success_count += 1
            except IngestNotificationError as e:
                failure_count += 1
                setattr(
                    e,
                    "metrics",
                    {"success": success_count, "failed": failure_count, "retries": 0},
                )
                raise
    else:
        structured_logger.info(
            event="pending_storage_scan_succeeded",
            status=EVENT_STATUS_SKIPPED,
            message="No pending invokations found.",
        )
    if with_metrics:
        return {
            "result": None,
            "metrics": {"success": success_count, "failed": failure_count, "retries": 0},
        }
    return None


def get_utc_logical_date_from_file(pending_marker: str) -> str:
    current_timezone_name = datetime.now(ZoneInfo("localtime")).tzname()
    structured_logger.info(
        event="notification_dispatch_started",
        status=EVENT_STATUS_STARTED,
        message=f"Current timezone: {current_timezone_name}",
    )
    structured_logger.info(
        event="notification_dispatch_started",
        status=EVENT_STATUS_STARTED,
        message=f"pending_marker : {pending_marker}",
    )
    timestamp = pending_marker.split("-")[1].split(".")[0]
    year = timestamp[0:4]
    month = timestamp[4:6]
    day = timestamp[6:8]
    hour = timestamp[8:10]
    minute = timestamp[10:12]
    dt_obj = datetime(int(year), int(month), int(day), int(hour), int(minute))
    dt_obj = dt_obj.replace(tzinfo=ZoneInfo("America/Sao_Paulo"))
    dt_utc = dt_obj.astimezone(ZoneInfo("UTC"))
    logical_date = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    return logical_date


def trigger_airflow_dag_run(
    config: ConfigDict,
    pending_marker: str,
    post_fn: Optional[Callable[..., Any]] = None,
) -> bool:
    """
    Sends the POST request to Airflow API using logical_date.
    """

    def get_config(config: ConfigDict) -> Tuple[str, AuthTuple]:
        user = config["AIRFLOW_USER"]
        password = config["AIRFLOW_PASSWORD"]
        airflow_webserver = config["AIRFLOW_WEBSERVER"]
        dag_name = config["AIRFLOW_DAG_NAME"]
        airflow_url = f"http://{airflow_webserver}:8080/api/v1/dags/{dag_name}/dagRuns"
        auth = (
            user,
            password,
        )

        return airflow_url, auth

    airflow_url, auth = get_config(config)
    structured_logger.info(
        event="notification_dispatch_started",
        status=EVENT_STATUS_STARTED,
        message=f"Airflow URL: {airflow_url}",
    )
    logical_date = get_utc_logical_date_from_file(pending_marker)
    structured_logger.info(
        event="notification_dispatch_started",
        status=EVENT_STATUS_STARTED,
        message=f"Triggering Airflow DAG for : {pending_marker} file and logical_date: {logical_date}",
    )
    payload = {
        "logical_date": logical_date,
        "conf": {
            "origin": "exctractloadlivedata",
            "marker_file": pending_marker,
        },
        "note": "Triggered by microservice after successful Raw upload",
    }
    structured_logger.info(
        event="notification_dispatch_started",
        status=EVENT_STATUS_STARTED,
        message=f"Request payload to be submitted: {payload}",
    )
    try:
        post_fn = post_fn or requests.post
        r = post_fn(
            airflow_url,
            json=payload,
            auth=auth,
            timeout=10,
        )
        structured_logger.info(
            event="notification_dispatch_succeeded",
            status=EVENT_STATUS_SUCCEEDED,
            message=f"Airflow API response for marker '{pending_marker}': {r.status_code} - {r.text}",
        )
        # 200 = Success, 409 = Already exists (safe to proceed)
        if r.status_code in [200, 409, 201]:
            return True
        raise IngestNotificationError(
            f"airflow dag trigger failed for marker '{pending_marker}' with status {r.status_code}"
        )
    except Exception as e:
        structured_logger.error(
            event="notification_dispatch_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Error triggering Airflow DAG for marker '{pending_marker}':",
        )
        structured_logger.error(
            event="notification_dispatch_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Exception details: {e}",
        )
        if isinstance(e, IngestNotificationError):
            raise
        raise IngestNotificationError(
            f"airflow dag trigger failed for marker '{pending_marker}'"
        ) from e
