from typing import Any, Callable, Dict, List, Tuple
from infra.sql_db_v2 import execute_select_query, execute_update_query
from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(
    service="transformlivedata",
    component="ingest_requests_processor",
    logger_name=__name__,
)


def _extract_database_config(config: Dict[str, Any]) -> Tuple[Dict[str, Any], str, str]:
    """
    Extract database connection and parse raw_events_table_name.

    Returns:
        Tuple of (connection_dict, schema_name, table_name)

    Raises:
        ValueError: if raw_events_table_name is not in 'schema.table' format
    """
    general = config["general"]
    raw_events_table = general["tables"]["raw_events_table_name"]
    if "." not in raw_events_table:
        raise ValueError(
            f"RAW_EVENTS_TABLE_NAME must be in 'schema.table' format. Got: '{raw_events_table}'"
        )
    schema, table = raw_events_table.split(".", 1)
    db = config["connections"]["database"]
    connection = {
        "host": db["host"],
        "port": db["port"],
        "database": db["database"],
        "user": db["user"],
        "password": db["password"],
    }
    return connection, schema, table


def get_unprocessed_requests(
    config: Dict[str, Any], select_fn: Callable[..., Any] = execute_select_query
) -> List[Dict[str, Any]]:
    """
    Get all unprocessed requests from the RAW_EVENTS_TABLE_NAME table.

    Queries the table defined in RAW_EVENTS_TABLE_NAME environment variable
    and retrieves all records where processed=false.

    Args:
        config: Configuration dictionary with RAW_EVENTS_TABLE_NAME and DB credentials

    Returns:
        list: List of unprocessed request records as dictionaries
              Each dictionary contains: id, filename, logical_date, processed, created_at, updated_at

    Raises:
        ValueError: if query execution fails
    """
    connection, schema, table = _extract_database_config(config)
    query = f'SELECT * FROM "{schema}"."{table}" WHERE processed = false ORDER BY created_at ASC'
    structured_logger.info(
        event="get_unprocessed_requests_started",
        message="Fetching unprocessed requests",
        status="STARTED",
        metadata={"schema": schema, "table": table},
    )
    try:
        results = select_fn(connection, query)
        if results:
            structured_logger.info(
                event="get_unprocessed_requests_succeeded",
                message="Unprocessed requests fetched",
                status="SUCCEEDED",
                metadata={"count": int(len(results)), "schema": schema, "table": table},
            )
        else:
            structured_logger.info(
                event="get_unprocessed_requests_succeeded",
                message="No unprocessed requests found",
                status="SUCCEEDED",
                metadata={"count": 0, "schema": schema, "table": table},
            )
        return results
    except Exception as e:
        structured_logger.error(
            event="get_unprocessed_requests_failed",
            message="Error fetching unprocessed requests",
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
            metadata={"schema": schema, "table": table},
        )
        raise ValueError(
            f"Error fetching unprocessed requests from {schema}.{table}: {e}"
        ) from e


def mark_request_as_processed(
    config: Dict[str, Any], logical_date: str, update_fn: Callable[..., Any] = execute_update_query
) -> bool:
    """
    Mark a processing request as processed by updating the processed field to true.

    Updates the record in RAW_EVENTS_TABLE_NAME where logical_date matches
    the provided logical_date parameter.

    Args:
        config: Configuration dictionary with RAW_EVENTS_TABLE_NAME and DB credentials
        logical_date: The logical_date (timestamp) of the request to mark as processed

    Returns:
        bool: True if update was successful, False otherwise
    """
    connection, schema, table = _extract_database_config(config)
    query = f"""
        UPDATE "{schema}"."{table}"
        SET processed = true, updated_at = NOW()
        WHERE logical_date = :logical_date
    """
    structured_logger.info(
        event="mark_request_as_processed_started",
        message="Marking request as processed by logical_date",
        status="STARTED",
        metadata={"logical_date": logical_date, "schema": schema, "table": table},
    )
    try:
        success = update_fn(connection, query, {"logical_date": logical_date})
        if success:
            structured_logger.info(
                event="mark_request_as_processed_succeeded",
                message="Request marked as processed by logical_date",
                status="SUCCEEDED",
                metadata={"logical_date": logical_date, "schema": schema, "table": table},
            )
        else:
            raise ValueError(
                f"Failed to mark request as processed for logical_date={logical_date}"
            )
        return success
    except Exception as e:
        structured_logger.error(
            event="mark_request_as_processed_failed",
            message="Error marking request as processed by logical_date",
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
            metadata={"logical_date": logical_date, "schema": schema, "table": table},
        )
        raise ValueError(
            f"Error marking request as processed for logical_date={logical_date}: {str(e)}"
        ) from e

def mark_request_as_processed_by_filename(
    config: Dict[str, Any], filename: str, update_fn: Callable[..., Any] = execute_update_query
) -> bool:
    """
    Mark a processing request as processed by updating the processed field to true.

    Updates the record in RAW_EVENTS_TABLE_NAME where filename matches
    the provided filename parameter.

    Args:
        config: Configuration dictionary with RAW_EVENTS_TABLE_NAME and DB credentials
        filename: The filename of the request to mark as processed

    Returns:
        bool: True if update was successful, False otherwise
    """
    connection, schema, table = _extract_database_config(config)
    query = f"""
        UPDATE "{schema}"."{table}"
        SET processed = true, updated_at = NOW()
        WHERE filename = :filename
    """
    structured_logger.info(
        event="mark_request_by_filename_started",
        message="Marking request as processed by filename",
        status="STARTED",
        metadata={"filename": filename, "schema": schema, "table": table},
    )
    try:
        success = update_fn(connection, query, {"filename": filename})
        if success:
            structured_logger.info(
                event="mark_request_by_filename_succeeded",
                message="Request marked as processed by filename",
                status="SUCCEEDED",
                metadata={"filename": filename, "schema": schema, "table": table},
            )
        else:
            raise ValueError(f"Failed to mark request as processed for filename={filename}")
        return success
    except Exception as e:
        structured_logger.error(
            event="mark_request_by_filename_failed",
            message="Error marking request as processed by filename",
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
            metadata={"filename": filename, "schema": schema, "table": table},
        )
        raise ValueError(f"Error marking request as processed for filename={filename}: {str(e)}") from e
