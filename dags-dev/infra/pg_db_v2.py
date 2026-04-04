import pandas as pd
import psycopg2
from psycopg2 import DatabaseError, InterfaceError
from psycopg2.extras import execute_values
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def get_db_connection(connection):
    """
    Return a new psycopg2 connection using connection dict.
    The function does NOT know anything about business logic / transformations.
    """
    conn = psycopg2.connect(
        host=connection["host"],
        port=connection["port"],
        dbname=connection["database"],
        user=connection["user"],
        password=connection["password"],
        sslmode=connection["sslmode"],
    )
    return conn


def bulk_insert_data_table(connection, sql, data_table):
    conn = None
    try:
        conn = get_db_connection(connection)
        cur = conn.cursor()
        execute_values(cur, sql, data_table, page_size=1000)
        conn.commit()
        logger.info(f"Successfully inserted {len(data_table)} rows into table")
    except (DatabaseError, InterfaceError) as db_err:
        if conn:
            conn.rollback()
        logger.error(f"Database error during insert into table: {db_err}")
        raise ValueError(f"Database error during insert into table: {db_err}")
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


def fetch_data_from_db_as_df(connection, sql):
    """
    Executes a SELECT query and returns the results as a Pandas DataFrame.
    """
    conn = None
    try:
        conn = get_db_connection(connection)
        df = pd.read_sql_query(sql, conn)
        return df
    except Exception as e:
        logger.error(f"Error fetching data to DataFrame: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")


def save_table_to_db(connection, schema, table_name, columns, buffer):
    try:
        conn = get_db_connection(connection)
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
        conn.commit()
    except (DatabaseError, InterfaceError) as db_err:
        if conn:
            conn.rollback()
        logger.error(f"Database error during insert into table: {db_err}")
        raise ValueError(f"Database error during insert into table: {db_err}")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Unexpected error during transformation: {e}")
        raise ValueError(f"Unexpected error during transformation: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")


def execute_sql_command(connection, sqls):
    """
    Executa um comando SQL simples (ex: TRUNCATE, DROP, DELETE)
    que não requer inserção de dados em massa ou retorno de DataFrame.
    """
    conn = None
    try:
        conn = get_db_connection(connection)
        cur = conn.cursor()
        for sql in sqls:
            cur.execute(sql)
        conn.commit()
        logger.info("SQL command executed successfully.")

    except (DatabaseError, InterfaceError) as db_err:
        if conn:
            conn.rollback()
        logger.error(f"Database error during SQL command execution: {db_err}")
        raise ValueError(f"Database error during SQL command execution: {db_err}")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Unexpected error executing SQL command: {e}")
        raise ValueError(f"Unexpected error executing SQL command: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.info("Database connection closed.")
