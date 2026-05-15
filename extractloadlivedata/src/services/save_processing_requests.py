from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.infra.sql_db_v2 import save_row
from src.infra.cache import (
    add_to_cache,
    get_from_cache,
    get_cache_value,
    remove_from_cache,
)
from src.infra.structured_logging import get_structured_logger
from src.domain.events import EVENT_STATUS_FAILED, EVENT_STATUS_RETRY, EVENT_STATUS_SKIPPED, EVENT_STATUS_STARTED, EVENT_STATUS_SUCCEEDED
from src.services.exceptions import IngestNotificationError

structured_logger = get_structured_logger(
    service="extractloadlivedata",
    component="save_processing_requests",
    logger_name=__name__,)

ConfigDict = Dict[str, Any]
DbConnection = Dict[str, Any]
PendingProcessingMetrics = Dict[str, int]
PendingProcessingResult = Dict[str, Any]


def create_pending_processing_request(
    config: ConfigDict,
    pending_marker: str,
    cache_factory: Optional[Callable[..., Any]] = None,
) -> None:
    """Add a pending processing request to the cache."""

    def get_config(config):
        cache_dir = config["PROCESSING_REQUESTS_CACHE_DIR"]
        return cache_dir

    structured_logger.debug(
        event="pending_storage_file_started",
        status=EVENT_STATUS_STARTED,
        message=f"Creating pending processing request for '{pending_marker}'",
    )
    # Use marker name without extension as key
    marker_name = f"{pending_marker.split('.')[0]}.pending"
    cache_dir = get_config(config)
    add_to_cache(cache_dir, marker_name, pending_marker, cache_factory=cache_factory)


def get_pending_processing_requests(
    config: ConfigDict,
    cache_factory: Optional[Callable[..., Any]] = None,
) -> List[Any]:
    """Retrieve all pending processing requests from the cache."""

    def get_config(config):
        cache_dir = config["PROCESSING_REQUESTS_CACHE_DIR"]
        return cache_dir

    cache_dir = get_config(config)
    return get_from_cache(cache_dir, cache_factory=cache_factory)


def remove_pending_processing_request(
    config: ConfigDict,
    marker_name: str,
    cache_factory: Optional[Callable[..., Any]] = None,
) -> None:
    """Remove a pending processing request from the cache."""

    def get_config(config):
        cache_dir = config["PROCESSING_REQUESTS_CACHE_DIR"]
        return cache_dir

    cache_dir = get_config(config)
    remove_from_cache(cache_dir, marker_name, cache_factory=cache_factory)


def get_utc_logical_date_from_file(pending_marker: str) -> datetime:
    """Extract logical date from filename and convert to UTC timezone-aware datetime."""
    try:
        filename_without_ext = pending_marker.split(".")[0]
        timestamp = filename_without_ext.split("-")[-1]
        year = int(timestamp[0:4])
        month = int(timestamp[4:6])
        day = int(timestamp[6:8])
        hour = int(timestamp[8:10])
        minute = int(timestamp[10:12])
        dt_obj = datetime(
            year, month, day, hour, minute, tzinfo=ZoneInfo("America/Sao_Paulo")
        )
        dt_utc = dt_obj.astimezone(ZoneInfo("UTC"))

        structured_logger.debug(
            event="get_utc_logical_date_succeeded",
            status=EVENT_STATUS_SUCCEEDED,
            message=f"Logical date extracted: {dt_utc}. From: {pending_marker}",
        )
        return dt_utc
    except Exception as e:
        structured_logger.error(
            event="get_utc_logical_date_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Error extracting logical date from file '{pending_marker}': {e}",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        raise


def save_processing_request(
    config: ConfigDict,
    pending_marker: str,
    save_row_fn: Optional[Callable[..., bool]] = None,
    engine_factory: Optional[Callable[..., Any]] = None,
) -> bool:
    """
    Save a processing request to the database.

    Args:
        config: Configuration dictionary
        pending_marker: Filename/marker for the processing request (e.g., 'posicoes_onibus-202602150936.json')

    Returns:
        bool: True if save was successful, False otherwise
    """

    def get_config(config: ConfigDict) -> Tuple[DbConnection, str, str]:
        if "RAW_EVENTS_TABLE_NAME" not in config:
            structured_logger.error(
                event="config_validation_failed",
                status=EVENT_STATUS_FAILED,
                message="RAW_EVENTS_TABLE_NAME configuration is missing.",
            )
            raise KeyError("RAW_EVENTS_TABLE_NAME configuration is missing.")
        raw_events_table = config["RAW_EVENTS_TABLE_NAME"]
        if "." not in raw_events_table:
            structured_logger.error(
                event="config_validation_failed",
                status=EVENT_STATUS_FAILED,
                message=f"RAW_EVENTS_TABLE_NAME must be in 'schema.table' format. Got: '{raw_events_table}'",
            )
            raise ValueError(
                "RAW_EVENTS_TABLE_NAME must be in 'schema.table' format."
            )
        schema, table = raw_events_table.split(".", 1)
        connection = {
            "host": config["DB_HOST"],
            "port": config["DB_PORT"],
            "database": config["DB_DATABASE"],
            "user": config["DB_USER"],
            "password": config["DB_PASSWORD"],
        }
        return connection, schema, table

    try:
        structured_logger.debug(
            event="notification_dispatch_started",
            status=EVENT_STATUS_STARTED,
            message=f"Saving processing request for: '{pending_marker}'",
        )
        connection, schema, table = get_config(config)
        # Get logical date from filename
        logical_date = get_utc_logical_date_from_file(pending_marker)
        # Get current UTC time for created_at and updated_at
        now_utc = datetime.now(timezone.utc)
        # Create tuple for the row: (filename, logical_date, processed, created_at, updated_at)
        row_tuple = (
            pending_marker,  # filename
            logical_date,  # logical_date
            False,  # processed (default: False)
            now_utc,  # created_at
            now_utc,  # updated_at
        )
        # Column names in the same order as row_tuple
        columns = ["filename", "logical_date", "processed", "created_at", "updated_at"]
        # Save to database using the generic function from sql_db module
        save_row_fn = save_row_fn or save_row
        success = save_row_fn(
            connection,
            schema,
            table,
            row_tuple,
            columns,
            engine_factory=engine_factory,
        )
        if success:
            structured_logger.info(
                event="db_storage_persist_succeeded",
                status=EVENT_STATUS_SUCCEEDED,
                message=f"Processing request saved successfully for marker '{pending_marker}' with logical_date '{logical_date}'",
            )
            return True
        structured_logger.error(
            event="notification_dispatch_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Failed to save processing request for marker '{pending_marker}'",
        )
        raise IngestNotificationError(
            f"failed to save processing request for marker '{pending_marker}'"
        )
    except Exception as e:
        structured_logger.error(
            event="notification_dispatch_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Unexpected error while saving processing request for marker '{pending_marker}': {e}",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        if isinstance(e, IngestNotificationError):
            raise
        raise IngestNotificationError(
            f"failed to save processing request for marker '{pending_marker}'"
        ) from e


def trigger_pending_processing_requests(
    config: ConfigDict,
    cache_factory: Optional[Callable[..., Any]] = None,
    save_fn: Optional[Callable[..., bool]] = None,
    with_metrics: bool = False,
) -> Optional[PendingProcessingResult]:
    """
    Process all pending processing requests and save them to the database.
    Only remove from cache if the database save was successful.
    """
    def get_config(config: ConfigDict) -> str:
        cache_dir = config["PROCESSING_REQUESTS_CACHE_DIR"]
        return cache_dir

    pending_markers = get_pending_processing_requests(
        config, cache_factory=cache_factory
    )
    cache_dir = get_config(config)
    success_count = 0
    failure_count = 0
    if pending_markers:
        for pending_marker_key in pending_markers:
            structured_logger.info(
                event="pending_storage_file_started",
                status=EVENT_STATUS_STARTED,
                message=f"Processing pending request: {pending_marker_key}",
            )
            pending_marker_value = get_cache_value(
                cache_dir, pending_marker_key, cache_factory=cache_factory
            )

            if pending_marker_value:
                save_fn = save_fn or save_processing_request
                try:
                    save_successful = save_fn(config, pending_marker_value)
                    if save_successful:
                        remove_pending_processing_request(
                            config, pending_marker_key, cache_factory=cache_factory
                        )
                        success_count += 1
                    else:
                        failure_count += 1
                        structured_logger.warning(
                            event="notification_dispatch_failed",
                            status=EVENT_STATUS_RETRY,
                            message=f"Failed to save processing request for marker '{pending_marker_value}'. Will retry on next execution.",
                        )
                        error = IngestNotificationError(
                            f"failed to save processing request for marker '{pending_marker_value}'"
                        )
                        setattr(
                            error,
                            "metrics",
                            {
                                "success": success_count,
                                "failed": failure_count,
                                "retries": 0,
                            },
                        )
                        raise error
                except IngestNotificationError as e:
                    if not getattr(e, "metrics", None):
                        failure_count += 1
                        structured_logger.warning(
                            event="notification_dispatch_failed",
                            status=EVENT_STATUS_RETRY,
                            message=f"Failed to save processing request for marker '{pending_marker_value}'. Will retry on next execution.",
                        )
                    setattr(
                        e,
                        "metrics",
                        {
                            "success": success_count,
                            "failed": failure_count,
                            "retries": 0,
                        },
                    )
                    raise
    else:
        structured_logger.info(
            event="pending_storage_scan_succeeded",
            status=EVENT_STATUS_SKIPPED,
            message="No pending processing requests found.",
        )
    if with_metrics:
        return {
            "result": None,
            "metrics": {"success": success_count, "failed": failure_count, "retries": 0},
        }
    return None
