from sqlalchemy import create_engine, text
from typing import Any, Callable, Dict, List, Optional, Tuple
from src.infra.structured_logging import get_structured_logger
from src.domain.events import EVENT_STATUS_FAILED, EVENT_STATUS_STARTED, EVENT_STATUS_SUCCEEDED

structured_logger = get_structured_logger(
    service="extractloadlivedata",
    component="sql_db_v2",
    logger_name=__name__,)


def save_row(connection: Dict[str, Any], schema: str, table: str, row_tuple: Tuple, columns: List[str], engine_factory: Optional[Callable[..., Any]] = None) -> bool:
    """
    Save a single row to the database.

    Args:
        connection: Dict with DB credentials
        schema: Schema name (e.g., 'to_be_processed')
        table: Table name (e.g., 'raw')
        row_tuple: Tuple with row values in the same order as columns
        columns: List of column names in the same order as row_tuple

    Returns:
        bool: True if save was successful, False otherwise
    """
    try:
        host = connection["host"]
        port = connection["port"]
        dbname = connection["database"]
        dbuser = connection["user"]
        password = connection["password"]
    except KeyError as e:
        structured_logger.error(
            event="config_validation_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Missing required connection key: {e}",
        )
        raise ValueError(f"Missing required connection key: {e}")

    try:
        db_uri = f"postgresql://{dbuser}:{password}@{host}:{port}/{dbname}"
        engine_factory = engine_factory or create_engine
        engine = engine_factory(db_uri)

        columns_str = ", ".join(f'"{col}"' for col in columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        insert_query = (
            f'INSERT INTO "{schema}"."{table}" ({columns_str}) VALUES ({placeholders})'
        )
        params = dict(zip(columns, row_tuple))

        structured_logger.info(
            event="storage_persist_started",
            status=EVENT_STATUS_STARTED,
            message=f"Executing INSERT into {schema}.{table}: {columns}",
        )
        with engine.begin() as conn:
            conn.execute(text(insert_query), params)

        structured_logger.info(
            event="storage_persist_succeeded",
            status=EVENT_STATUS_SUCCEEDED,
            message=f"Row inserted successfully into {schema}.{table}",
        )
        return True
    except Exception as e:
        structured_logger.error(
            event="storage_persist_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Database error while saving row to {schema}.{table}: {e}",
        )
        raise ValueError(f"Database error while saving row to {schema}.{table}: {e}")


def execute_select_query(connection: Dict[str, Any], query: str, engine_factory: Optional[Callable[..., Any]] = None) -> List[Dict[str, Any]]:
    """
    Execute a SELECT query and return results as a list of dictionaries.
    """
    try:
        host = connection["host"]
        port = connection["port"]
        dbname = connection["database"]
        dbuser = connection["user"]
        password = connection["password"]
    except KeyError as e:
        structured_logger.error(
            event="config_validation_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Missing required connection key: {e}",
        )
        raise ValueError(f"Missing required connection key: {e}")

    try:
        db_uri = f"postgresql://{dbuser}:{password}@{host}:{port}/{dbname}"
        engine_factory = engine_factory or create_engine
        engine = engine_factory(db_uri)

        structured_logger.info(
            event="storage_persist_started",
            status=EVENT_STATUS_STARTED,
            message=f"Executing SELECT query: {query[:100]}...",
        )
        with engine.begin() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            rows_as_dicts = [dict(row._mapping) for row in rows]
        structured_logger.info(
            event="storage_persist_succeeded",
            status=EVENT_STATUS_SUCCEEDED,
            message=f"Query returned {len(rows_as_dicts)} row(s)",
        )
        return rows_as_dicts
    except Exception as e:
        structured_logger.error(
            event="storage_persist_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Database error while executing SELECT query: {e}",
        )
        raise ValueError(f"Database error while executing SELECT query: {e}")


def execute_update_query(connection: Dict[str, Any], query: str, params: Optional[Dict[str, Any]] = None, engine_factory: Optional[Callable[..., Any]] = None) -> bool:
    """
    Execute an UPDATE, DELETE, or other DML query.
    """
    try:
        host = connection["host"]
        port = connection["port"]
        dbname = connection["database"]
        dbuser = connection["user"]
        password = connection["password"]
    except KeyError as e:
        structured_logger.error(
            event="config_validation_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Missing required connection key: {e}",
        )
        raise ValueError(f"Missing required connection key: {e}")

    try:
        db_uri = f"postgresql://{dbuser}:{password}@{host}:{port}/{dbname}"
        engine_factory = engine_factory or create_engine
        engine = engine_factory(db_uri)

        structured_logger.info(
            event="storage_persist_started",
            status=EVENT_STATUS_STARTED,
            message=f"Executing UPDATE query: {query[:100]}...",
        )
        with engine.begin() as conn:
            if params:
                conn.execute(text(query), params)
            else:
                conn.execute(text(query))

        structured_logger.info(
            event="storage_persist_succeeded",
            status=EVENT_STATUS_SUCCEEDED,
            message="Update query executed successfully",
        )
        return True
    except Exception as e:
        structured_logger.error(
            event="storage_persist_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Database error while executing UPDATE query: {e}",
        )
        raise ValueError(f"Database error while executing UPDATE query: {e}")
