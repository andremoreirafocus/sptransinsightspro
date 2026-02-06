import duckdb
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def get_duckdb_connection(config):
    def get_config(config):
        # Using .get() with fallback or checking existence to avoid KeyErrors
        try:
            connection_data = {
                "minio_endpoint": config["MINIO_ENDPOINT"],
                "access_key": config["ACCESS_KEY"],
                "secret_key": config["SECRET_KEY"],
                "secure": False,
            }
            return connection_data
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    connection_data = get_config(config)
    con = duckdb.connect(database=":memory:")
    con.execute(f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_endpoint='{connection_data["minio_endpoint"]}'; 
        SET s3_access_key_id='{connection_data["access_key"]}';
        SET s3_secret_access_key='{connection_data["secret_key"]}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con
