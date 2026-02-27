from sqlalchemy import create_engine, text
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def save_dataframe_to_db(config, df, full_table_name):
    def get_config(config):
        try:
            host = config["DB_HOST"]
            port = config["DB_PORT"]
            dbname = config["DB_DATABASE"]
            dbuser = config["DB_USER"]
            password = config["DB_PASSWORD"]
            return (host, port, dbname, dbuser, password)
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    (host, port, dbname, dbuser, password) = get_config(config)
    db_uri = f"postgresql://{dbuser}:{password}@{host}:{port}/{dbname}"
    engine = create_engine(db_uri)
    schema = full_table_name.split(".")[0]
    table_name = full_table_name.split(".")[1]
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
    )


def update_db_table_with_dataframe(config, df, full_table_name):
    def get_config(config):
        try:
            host = config["DB_HOST"]
            port = config["DB_PORT"]
            dbname = config["DB_DATABASE"]
            dbuser = config["DB_USER"]
            password = config["DB_PASSWORD"]
            return (host, port, dbname, dbuser, password)
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    (host, port, dbname, dbuser, password) = get_config(config)
    db_uri = f"postgresql://{dbuser}:{password}@{host}:{port}/{dbname}"
    schema = full_table_name.split(".")[0]
    table_name = full_table_name.split(".")[1]
    engine = create_engine(db_uri)
    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE refined."{table_name}"'))
        df.to_sql(
            name=table_name,
            con=conn,
            schema=schema,
            if_exists="append",
            index=False,
        )
        # Update statistics so query stays fast
        conn.execute(text(f'ANALYZE {schema}."{table_name}"'))


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


def execute_select_query(config, query):
    """
    Execute a SELECT query and return results as a list of dictionaries.

    Args:
        config: Configuration dictionary with DB credentials
        query: SQL SELECT query as a string

    Returns:
        list: List of rows as dictionaries, empty list if no results or error occurs
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

        logger.info(f"Executing SELECT query: {query[:100]}...")
        # Execute query and fetch results
        with engine.begin() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            # Convert rows to list of dictionaries
            rows_as_dicts = [dict(row._mapping) for row in rows]
        logger.info(f"Query returned {len(rows_as_dicts)} row(s)")
        return rows_as_dicts
    except Exception as e:
        logger.error(f"Database error while executing SELECT query: {e}", exc_info=True)
        return []


def execute_update_query(config, query, params=None):
    """
    Execute an UPDATE, DELETE, or other DML query.

    Args:
        config: Configuration dictionary with DB credentials
        query: SQL UPDATE/DELETE query as a string
        params: Optional dictionary of named parameters for the query

    Returns:
        bool: True if update was successful, False otherwise
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

        logger.info(f"Executing UPDATE query: {query[:100]}...")

        # Execute query with transaction management
        with engine.begin() as conn:
            if params:
                conn.execute(text(query), params)
            else:
                conn.execute(text(query))

        logger.info("Update query executed successfully")
        return True
    except Exception as e:
        logger.error(f"Database error while executing UPDATE query: {e}", exc_info=True)
        return False
