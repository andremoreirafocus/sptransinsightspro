from infra.duck_db_v3 import get_duckdb_connection
import logging
from typing import Any, Callable, Dict, Tuple
import pandas as pd

logger = logging.getLogger(__name__)


def load_trip_details(
    config: Dict[str, Any],
    get_duckdb_connection_fn: Callable[[Dict[str, Any]], Any] = get_duckdb_connection,
) -> pd.DataFrame:
    def get_config(config: Dict[str, Any]) -> Tuple[Dict[str, Any], str, str, str]:
        general = config["general"]
        connections = config["connections"]
        storage = general["storage"]
        tables = general["tables"]
        bucket_name = storage["trusted_bucket"]
        gtfs_folder = storage["gtfs_folder"]
        trip_details_table_name = tables["trip_details_table_name"]
        connection = {
            **connections["object_storage"],
            "secure": False,
        }
        return connection, bucket_name, gtfs_folder, trip_details_table_name

    connection, bucket_name, gtfs_folder, trip_details_table_name = get_config(config)
    s3_path = f"s3://{bucket_name}/{gtfs_folder}/{trip_details_table_name}/{trip_details_table_name}.parquet"
    logger.info(f"Loading trip_details from storage: {s3_path}")
    try:
        con = get_duckdb_connection_fn(connection)
        df = con.execute(f"SELECT * FROM read_parquet('{s3_path}')").df()
        logger.info(f"Successfully loaded {df.shape[0]} trips from storage.")
        return df
    except Exception as e:
        logger.error("Error fetching trip_details from storage '%s': %s", s3_path, e)
        raise ValueError(f"Error fetching trip_details from storage: {e}") from e
    finally:
        if "con" in locals():
            con.close()
