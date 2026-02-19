from infra.duck_db import get_duckdb_connection
import logging

logger = logging.getLogger(__name__)


def load_trip_details_from_storage_to_dataframe(config):
    def get_config(config):
        try:
            bucket_name = config["TRUSTED_BUCKET"]
            gtfs_folder = config["GTFS_FOLDER"]
            trip_details_table_name = config["TRIP_DETAILS_TABLE_NAME"]
            if len(trip_details_table_name.split(".")) == 2:
                trip_details_table_name = trip_details_table_name.split(".")[1]
            return bucket_name, gtfs_folder, trip_details_table_name
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    bucket_name, gtfs_folder, trip_details_table_name = get_config(config)
    s3_path = f"s3://{bucket_name}/{gtfs_folder}/{trip_details_table_name}/{trip_details_table_name}.parquet"
    logger.info(f"Loading trip_details from storage: {s3_path}")
    con = None
    try:
        con = get_duckdb_connection(config)
        df = con.execute(f"SELECT * FROM read_parquet('{s3_path}')").df()
        logger.info(f"Successfully loaded {df.shape[0]} trips from storage.")
        return df
    except Exception as e:
        logger.error(f"Error fetching trip_details from storage: {e}")
        raise
    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed.")
