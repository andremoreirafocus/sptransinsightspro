from sqlalchemy import create_engine, text
import logging

logger = logging.getLogger(__name__)


def save_row(config, schema, table, row_tuple, columns):
    """
    Save a single row to the database.

    This is a generic function that handles all database operations.
    Creates connection, inserts row, closes connection immediately.

    Args:
        config: Configuration dictionary with DB credentials
        schema: Schema name (e.g., 'to_be_processed')
        table: Table name (e.g., 'raw')
        row_tuple: Tuple with row values in the same order as columns
        columns: List of column names in the same order as row_tuple
                (e.g., ['filename', 'logical_date', 'processed', 'created_at', 'updated_at'])

    Returns:
        bool: True if save was successful, False otherwise
    """

    def get_config(config):
        """Extract database configuration from config object."""
        host = config["DB_HOST"]
        port = config["DB_PORT"]
        dbname = config["DB_DATABASE"]
        dbuser = config["DB_USER"]
        password = config["DB_PASSWORD"]
        return host, port, dbname, dbuser, password

    try:
        # Get database configuration
        host, port, dbname, dbuser, password = get_config(config)

        # Build database URI
        db_uri = f"postgresql://{dbuser}:{password}@{host}:{port}/{dbname}"

        # Create engine and connection for this specific operation
        engine = create_engine(db_uri)

        # Build INSERT statement with named parameters
        columns_str = ", ".join(f'"{col}"' for col in columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        insert_query = (
            f'INSERT INTO "{schema}"."{table}" ({columns_str}) VALUES ({placeholders})'
        )

        # Create dictionary of named parameters
        params = dict(zip(columns, row_tuple))

        logger.info(f"Executing INSERT into {schema}.{table}: {columns}")

        # Execute using SQLAlchemy's text() with transaction management
        with engine.begin() as conn:
            conn.execute(text(insert_query), params)

        logger.info(f"Row inserted successfully into {schema}.{table}")
        return True

    except Exception as e:
        logger.error(
            f"Database error while saving row to {schema}.{table}: {e}",
            exc_info=True,
        )
        return False
