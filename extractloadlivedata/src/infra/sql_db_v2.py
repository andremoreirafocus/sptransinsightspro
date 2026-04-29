from sqlalchemy import create_engine, text
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


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
        logger.error(f"Missing required connection key: {e}")
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

        logger.info(f"Executing INSERT into {schema}.{table}: {columns}")
        with engine.begin() as conn:
            conn.execute(text(insert_query), params)

        logger.info(f"Row inserted successfully into {schema}.{table}")
        return True
    except Exception as e:
        logger.error(
            f"Database error while saving row to {schema}.{table}: {e}",
            exc_info=True,
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
        logger.error(f"Missing required connection key: {e}")
        raise ValueError(f"Missing required connection key: {e}")

    try:
        db_uri = f"postgresql://{dbuser}:{password}@{host}:{port}/{dbname}"
        engine_factory = engine_factory or create_engine
        engine = engine_factory(db_uri)

        logger.info(f"Executing SELECT query: {query[:100]}...")
        with engine.begin() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            rows_as_dicts = [dict(row._mapping) for row in rows]
        logger.info(f"Query returned {len(rows_as_dicts)} row(s)")
        return rows_as_dicts
    except Exception as e:
        logger.error(f"Database error while executing SELECT query: {e}", exc_info=True)
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
        logger.error(f"Missing required connection key: {e}")
        raise ValueError(f"Missing required connection key: {e}")

    try:
        db_uri = f"postgresql://{dbuser}:{password}@{host}:{port}/{dbname}"
        engine_factory = engine_factory or create_engine
        engine = engine_factory(db_uri)

        logger.info(f"Executing UPDATE query: {query[:100]}...")
        with engine.begin() as conn:
            if params:
                conn.execute(text(query), params)
            else:
                conn.execute(text(query))

        logger.info("Update query executed successfully")
        return True
    except Exception as e:
        logger.error(f"Database error while executing UPDATE query: {e}", exc_info=True)
        raise ValueError(f"Database error while executing UPDATE query: {e}")
