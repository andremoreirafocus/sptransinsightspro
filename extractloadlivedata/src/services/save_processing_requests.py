from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import logging

from src.infra.sql_db import save_row
from src.infra.cache import (
    add_to_cache,
    get_from_cache,
    get_cache_value,
    remove_from_cache,
)

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def create_pending_processing_request(config, pending_marker):
    """Add a pending processing request to the cache."""
    logger.info(f"Creating pending processing request for marker '{pending_marker}'")
    # Use marker name without extension as key
    marker_name = f"{pending_marker.split('.')[0]}.pending"
    add_to_cache(config, marker_name, pending_marker)


def get_pending_processing_requests(config):
    """Retrieve all pending processing requests from the cache."""
    return get_from_cache(config)


def remove_pending_processing_request(config, marker_name):
    """Remove a pending processing request from the cache."""
    remove_from_cache(config, marker_name)


def get_utc_logical_date_from_file(pending_marker):
    """Extract logical date from filename and convert to UTC timezone-aware datetime."""
    try:
        logger.info(f"Extracting logical date from pending_marker: {pending_marker}")
        # Remove extension(s) to get the timestamp
        # e.g., "posicoes_onibus-202602150936.json" or "posicoes_onibus-202602150936.json.zst"
        filename_without_ext = pending_marker.split(".")[0]
        timestamp = filename_without_ext.split("-")[-1]
        # Parse timestamp: YYYYMMDDHHMM format
        year = int(timestamp[0:4])
        month = int(timestamp[4:6])
        day = int(timestamp[6:8])
        hour = int(timestamp[8:10])
        minute = int(timestamp[10:12])

        # Create datetime in São Paulo timezone
        dt_obj = datetime(
            year, month, day, hour, minute, tzinfo=ZoneInfo("America/Sao_Paulo")
        )

        # Convert to UTC
        dt_utc = dt_obj.astimezone(ZoneInfo("UTC"))

        logger.info(f"Logical date extracted: {dt_utc}")
        return dt_utc
    except Exception as e:
        logger.error(
            f"Error extracting logical date from file '{pending_marker}': {e}",
            exc_info=True,
        )
        raise


def save_processing_request(config, pending_marker):
    """
    Save a processing request to the database.

    Args:
        config: Configuration dictionary
        pending_marker: Filename/marker for the processing request (e.g., 'posicoes_onibus-202602150936.json')

    Returns:
        bool: True if save was successful, False otherwise
    """
    try:
        logger.info(f"Saving processing request for marker '{pending_marker}'")
        # Parse RAW_EVENTS_TABLE_NAME to get schema and table
        if "RAW_EVENTS_TABLE_NAME" not in config:
            logger.error("RAW_EVENTS_TABLE_NAME configuration is missing.")
            return False
        raw_events_table = config["RAW_EVENTS_TABLE_NAME"]
        if "." not in raw_events_table:
            logger.error(
                f"RAW_EVENTS_TABLE_NAME must be in 'schema.table' format. Got: '{raw_events_table}'"
            )
            return False
        schema, table = raw_events_table.split(".", 1)
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
        success = save_row(config, schema, table, row_tuple, columns)
        if success:
            logger.info(
                f"Processing request saved successfully for marker '{pending_marker}' with logical_date '{logical_date}'"
            )
        else:
            logger.error(
                f"Failed to save processing request for marker '{pending_marker}'"
            )
        return success
    except Exception as e:
        logger.error(
            f"Unexpected error while saving processing request for marker '{pending_marker}': {e}",
            exc_info=True,
        )
        return False


def trigger_pending_processing_requests(config):
    """
    Process all pending processing requests and save them to the database.
    Only remove from cache if the database save was successful.
    """
    pending_markers = get_pending_processing_requests(config)
    if pending_markers:
        for pending_marker_key in pending_markers:
            logger.info(f"Processing pending request: {pending_marker_key}")
            pending_marker_value = get_cache_value(config, pending_marker_key)

            if pending_marker_value:
                if save_processing_request(config, pending_marker_value):
                    remove_pending_processing_request(config, pending_marker_key)
                else:
                    logger.warning(
                        f"Failed to save processing request for marker '{pending_marker_value}'. Will retry on next execution."
                    )
    else:
        logger.info("No pending processing requests found.")
