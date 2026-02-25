from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import diskcache as dc
import os
import logging

from src.infra.sql_db import save_row

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)

# Global cache for pending processing requests
_processing_requests_cache = None


def get_processing_requests_cache(config):
    """Get or initialize the diskcache instance for pending processing requests."""

    def get_config(config):
        CACHE_DIR = config["PROCESSING_REQUESTS_CACHE_DIR"]
        return CACHE_DIR

    global _processing_requests_cache
    CACHE_DIR = get_config(config)
    if _processing_requests_cache is None:
        os.makedirs(CACHE_DIR, exist_ok=True)
        _processing_requests_cache = dc.Cache(CACHE_DIR)
    return _processing_requests_cache


def create_pending_processing_request(config, pending_marker):
    """Add a pending processing request to the cache."""
    logger.info(f"Creating pending processing request for marker '{pending_marker}'")
    cache = get_processing_requests_cache(config)

    # Use marker name without extension as key
    marker_name = f"{pending_marker.split('.')[0]}.pending"
    cache[marker_name] = pending_marker

    logger.info(
        f"Pending processing request created in cache with key '{marker_name}' and value '{pending_marker}'"
    )


def get_pending_processing_requests(config):
    """Retrieve all pending processing requests from the cache."""
    logger.info("Checking for pending processing requests...")
    cache = get_processing_requests_cache(config)
    pending_markers = sorted(list(cache))
    logger.info(f"Found {len(pending_markers)} pending processing request(s).")
    return pending_markers


def remove_pending_processing_request(config, marker_name):
    """Remove a pending processing request from the cache."""
    logger.info(f"Removing pending processing request marker '{marker_name}'")
    cache = get_processing_requests_cache(config)
    if marker_name in cache:
        del cache[marker_name]
        logger.info(
            f"Pending processing request marker '{marker_name}' removed successfully."
        )
    else:
        logger.warning(
            f"Pending processing request marker '{marker_name}' not found in cache."
        )


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
            cache = get_processing_requests_cache(config)
            pending_marker_value = cache.get(pending_marker_key)

            if pending_marker_value:
                if save_processing_request(config, pending_marker_value):
                    remove_pending_processing_request(config, pending_marker_key)
                else:
                    logger.warning(
                        f"Failed to save processing request for marker '{pending_marker_value}'. Will retry on next execution."
                    )
    else:
        logger.info("No pending processing requests found.")
