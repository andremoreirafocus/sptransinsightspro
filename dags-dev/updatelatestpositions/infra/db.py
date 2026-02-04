import pandas as pd
import psycopg2
from psycopg2 import DatabaseError, InterfaceError
from psycopg2.extras import execute_values
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


def bulk_insert_data_table(config, sql, data_table):
    conn = None
    try:
        # 1. Initialize connection and cursor
        conn = get_db_connection(config)
        cur = conn.cursor()

        # 2. Execute the batch insert
        execute_values(cur, sql, data_table, page_size=1000)

        # 3. Commit only if execution succeeds
        conn.commit()
        logger.info(f"Successfully inserted {len(data_table)} rows into table")
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
            logger.info("Database connection closed.")


def fetch_data_from_db_as_df(config, sql):
    """
    Executes a SELECT query and returns the results as a Pandas DataFrame.
    """
    conn = None
    try:
        conn = get_db_connection(config)
        # pd.read_sql_query handles the cursor and fetching automatically
        df = pd.read_sql_query(sql, conn)
        # print(df.head(5))
        # print(df.tail(5))
        print(df.shape)
        return df
    except Exception as e:
        logger.error(f"Error fetching data to DataFrame: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")
