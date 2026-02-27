import logging
from infra.sql_db import execute_select_query, execute_update_query

logger = logging.getLogger(__name__)


def get_unprocessed_requests(config):
    """
    Get all unprocessed requests from the RAW_EVENTS_TABLE_NAME table.

    Queries the table defined in RAW_EVENTS_TABLE_NAME environment variable
    and retrieves all records where processed=false.

    Args:
        config: Configuration dictionary with RAW_EVENTS_TABLE_NAME and DB credentials

    Returns:
        list: List of unprocessed request records as dictionaries
              Each dictionary contains: id, filename, logical_date, processed, created_at, updated_at
              Returns empty list if table not found or query fails
    """

    def get_config(config):
        """Extract RAW_EVENTS_TABLE_NAME and parse schema and table."""
        if "RAW_EVENTS_TABLE_NAME" not in config:
            raise KeyError("RAW_EVENTS_TABLE_NAME configuration is missing.")

        raw_events_table = config["RAW_EVENTS_TABLE_NAME"]
        if "." not in raw_events_table:
            raise ValueError(
                f"RAW_EVENTS_TABLE_NAME must be in 'schema.table' format. Got: '{raw_events_table}'"
            )

        schema, table = raw_events_table.split(".", 1)
        return schema, table

    try:
        # Get schema and table from config
        schema, table = get_config(config)

        # Build SELECT query for unprocessed requests
        query = f'SELECT * FROM "{schema}"."{table}" WHERE processed = false ORDER BY created_at ASC'

        logger.info(f"Fetching unprocessed requests from {schema}.{table}")
        print(f"Fetching unprocessed requests from {schema}.{table}")

        # Execute query
        results = execute_select_query(config, query)

        if results:
            logger.info(f"Found {len(results)} unprocessed request(s)")
        else:
            logger.info("No unprocessed requests found")

        return results

    except Exception as e:
        logger.error(f"Error while fetching unprocessed requests: {e}", exc_info=True)
        return []


def mark_request_as_processed(config, logical_date):
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

    def get_config(config):
        """Extract RAW_EVENTS_TABLE_NAME and parse schema and table."""
        if "RAW_EVENTS_TABLE_NAME" not in config:
            raise KeyError("RAW_EVENTS_TABLE_NAME configuration is missing.")

        raw_events_table = config["RAW_EVENTS_TABLE_NAME"]
        if "." not in raw_events_table:
            raise ValueError(
                f"RAW_EVENTS_TABLE_NAME must be in 'schema.table' format. Got: '{raw_events_table}'"
            )

        schema, table = raw_events_table.split(".", 1)
        return schema, table

    try:
        # Get schema and table from config
        schema, table = get_config(config)

        # Build UPDATE query
        query = f"""
            UPDATE "{schema}"."{table}"
            SET processed = true, updated_at = NOW()
            WHERE logical_date = :logical_date
        """

        logger.info(
            f"Marking request as processed for logical_date: {logical_date} in {schema}.{table}"
        )

        # Execute update
        success = execute_update_query(config, query, {"logical_date": logical_date})

        if success:
            logger.info(f"Request with logical_date={logical_date} marked as processed")
        else:
            logger.error(
                f"Failed to mark request as processed for logical_date={logical_date}"
            )

        return success

    except Exception as e:
        logger.error(f"Error while marking request as processed: {e}", exc_info=True)
        return False


def mark_request_as_processed_by_filename(config, filename):
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

    def get_config(config):
        """Extract RAW_EVENTS_TABLE_NAME and parse schema and table."""
        if "RAW_EVENTS_TABLE_NAME" not in config:
            raise KeyError("RAW_EVENTS_TABLE_NAME configuration is missing.")

        raw_events_table = config["RAW_EVENTS_TABLE_NAME"]
        if "." not in raw_events_table:
            raise ValueError(
                f"RAW_EVENTS_TABLE_NAME must be in 'schema.table' format. Got: '{raw_events_table}'"
            )

        schema, table = raw_events_table.split(".", 1)
        return schema, table

    try:
        # Get schema and table from config
        schema, table = get_config(config)

        # Build UPDATE query
        query = f"""
            UPDATE "{schema}"."{table}"
            SET processed = true, updated_at = NOW()
            WHERE filename = :filename
        """

        logger.info(
            f"Marking request as processed for filename: {filename} in {schema}.{table}"
        )

        # Execute update
        success = execute_update_query(config, query, {"filename": filename})

        if success:
            logger.info(f"Request with filename={filename} marked as processed")
        else:
            logger.error(f"Failed to mark request as processed for filename={filename}")

        return success

    except Exception as e:
        logger.error(f"Error while marking request as processed: {e}", exc_info=True)
        return False
