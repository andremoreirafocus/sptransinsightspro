from infra.duck_db_v2 import get_duckdb_connection
import logging
from typing import Any, Dict, Tuple
import pandas as pd

logger = logging.getLogger(__name__)


def load_trip_details(config: Dict[str, Any]) -> pd.DataFrame:
    def get_config(
        config: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], str, str, str]:
        try:
            general = config["general"]
            connections = config["connections"]
            storage = general["storage"]
            tables = general["tables"]
            bucket_name = storage["trusted_bucket"]
            gtfs_folder = storage["gtfs_folder"]
            trip_details_table_name = tables["trip_details_table_name"]
            object_storage = connections["object_storage"]
            connection = {
                "minio_endpoint": object_storage["endpoint"],
                "access_key": object_storage["access_key"],
                "secret_key": object_storage["secret_key"],
                "secure": False,
            }
            return connection, bucket_name, gtfs_folder, trip_details_table_name
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise ValueError(f"Missing required configuration key: {e}")

    connection, bucket_name, gtfs_folder, trip_details_table_name = get_config(config)
    s3_path = f"s3://{bucket_name}/{gtfs_folder}/{trip_details_table_name}/{trip_details_table_name}.parquet"
    logger.info(f"Loading trip_details from storage: {s3_path}")
    con = None
    try:
        con = get_duckdb_connection(connection)
        df = con.execute(f"SELECT * FROM read_parquet('{s3_path}')").df()
        logger.info(f"Successfully loaded {df.shape[0]} trips from storage.")
        return df
    except Exception as e:
        logger.error(f"Error fetching trip_details from storage: {e}")
        raise RuntimeError(f"Error fetching trip_details from storage: {e}")
    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed.")
