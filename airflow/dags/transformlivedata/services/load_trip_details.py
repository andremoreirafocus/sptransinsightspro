from infra.duck_db_v3 import get_duckdb_connection
from observability.structured_event_logger import get_structured_logger
from typing import Any, Callable, Dict, Tuple
import pandas as pd

structured_logger = get_structured_logger(
    service="transformlivedata",
    component="load_trip_details",
    logger_name=__name__,
)


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
    structured_logger.info(
        event="load_trip_details_started",
        message="Loading trip details from storage",
        status="STARTED",
        metadata={"s3_path": s3_path},
    )
    try:
        con = get_duckdb_connection_fn(connection)
        df = con.execute(f"SELECT * FROM read_parquet('{s3_path}')").df()
        structured_logger.info(
            event="load_trip_details_succeeded",
            message="Trip details loaded from storage",
            status="SUCCEEDED",
            metadata={"s3_path": s3_path, "rows_loaded": int(df.shape[0])},
        )
        return df
    except Exception as e:
        structured_logger.error(
            event="load_trip_details_failed",
            message="Error fetching trip details from storage",
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
            metadata={"s3_path": s3_path},
        )
        raise ValueError(f"Error fetching trip_details from storage: {e}") from e
    finally:
        if "con" in locals():
            con.close()
