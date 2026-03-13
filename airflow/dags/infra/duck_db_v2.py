import duckdb
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def get_duckdb_connection(connection):
    try:
        minio_endpoint = connection["minio_endpoint"]
        access_key = connection["access_key"]
        secret_key = connection["secret_key"]
    except KeyError as e:
        logger.error(f"Missing required connection key: {e}")
        raise
    con = duckdb.connect(database=":memory:")
    con.execute(f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_endpoint='{minio_endpoint}'; 
        SET s3_access_key_id='{access_key}';
        SET s3_secret_access_key='{secret_key}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con
