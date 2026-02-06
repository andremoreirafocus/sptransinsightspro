import psycopg2
from psycopg2 import DatabaseError, InterfaceError
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def get_db_connection(config):
    """
    Return a new psycopg2 connection using environment variables.
    The function does NOT know anything about business logic / transformations.
    """
    conn = psycopg2.connect(
        host=config["DB_HOST"],
        port=config["DB_PORT"],
        dbname=config["DB_DATABASE"],
        user=config["DB_USER"],
        password=config["DB_PASSWORD"],
        sslmode=config.get("DB_SSLMODE", "prefer"),
    )  # psycopg2.connect supports keyword arguments for connection parameters.[web:5][web:8]
    return conn


def save_table_to_db(config, table_name, columns, buffer):
    def get_config(config):
        schema = config["SCHEMA"]
        return schema

    try:
        # 1. Initialize connection and cursor
        schema = get_config(config)
        conn = get_db_connection(config)
        cur = conn.cursor()
        cur.execute(f"""
            SELECT schemaname, tablename
            FROM pg_tables
            WHERE schemaname = '{schema}' AND tablename = '{table_name}';
        """)
        if not cur.fetchone():
            raise ValueError(
                f"Table {schema}.{table_name} does not exist. Create it first: CREATE TABLE {schema}.{table_name} (...);"
            )
        full_table_name = f"{schema}.{table_name}"
        copy_sql = f"""
        COPY {full_table_name} ({", ".join(columns)})
        FROM STDIN WITH (FORMAT CSV, DELIMITER ',', NULL '')
        """
        cur.copy_expert(copy_sql, buffer)
        # 3. Commit only if execution succeeds
        conn.commit()
        # print(f"Successfully inserted {len(data_table)} rows into table")
    except (DatabaseError, InterfaceError) as db_err:
        # Rollback the transaction if any database error occurs
        if conn:
            conn.rollback()
        logger.error(f"Database error during insert into table: {db_err}")
        raise  # Re-raise so the orchestrator knows the pipeline failed
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Unexpected error during transformation: {e}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")
