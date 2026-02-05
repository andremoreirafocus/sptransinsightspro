import duckdb
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def load_trip_details_from_storage_to_dataframe(config):
    def get_config(config):
        try:
            bucket_name = config["TRUSTED_BUCKET"]
            gtfs_folder = config["GTFS_FOLDER"]
            trip_details_table_name = config["TRIP_DETAILS_TABLE_NAME"]
            connection_data = {
                "minio_endpoint": config["MINIO_ENDPOINT"],
                "access_key": config["ACCESS_KEY"],
                "secret_key": config["SECRET_KEY"],
                "secure": False,
            }
            return bucket_name, gtfs_folder, trip_details_table_name, connection_data
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    bucket_name, gtfs_folder, trip_details_table_name, connection_data = get_config(
        config
    )
    # Path where you exported the data in the previous step
    s3_path = f"s3://{bucket_name}/{gtfs_folder}/{trip_details_table_name}/{trip_details_table_name}.parquet"
    logger.info(f"Loading trip_details from MinIO: {s3_path}")
    con = None
    try:
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
        df = con.execute(f"SELECT * FROM read_parquet('{s3_path}')").df()
        logger.info(f"Successfully loaded {df.shape[0]} trips from MinIO.")
        return df
    except Exception as e:
        logger.error(f"Error fetching trip_details from MinIO: {e}")
        raise
    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed.")
