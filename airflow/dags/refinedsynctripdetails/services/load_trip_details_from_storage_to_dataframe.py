from infra.duck_db_v3 import get_duckdb_connection
from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)


def load_trip_details_from_storage_to_dataframe(config, duckdb_client=None):
    def get_config(config):
        storage = config["general"]["storage"]
        tables = config["general"]["tables"]
        bucket_name = storage["trusted_bucket"]
        gtfs_folder = storage["gtfs_folder"]
        trip_details_table_name = tables["trip_details_table_name"]
        if len(trip_details_table_name.split(".")) == 2:
            trip_details_table_name = trip_details_table_name.split(".")[1]
        connection = {
            **config["connections"]["object_storage"],
            "secure": False,
        }
        return connection, bucket_name, gtfs_folder, trip_details_table_name

    connection, bucket_name, gtfs_folder, trip_details_table_name = get_config(config)
    s3_path = f"s3://{bucket_name}/{gtfs_folder}/{trip_details_table_name}/{trip_details_table_name}.parquet"
    structured_logger.info(
        event="trip_details_load_started",
        message="Loading trip details from storage",
        metadata={"path": s3_path},
    )
    con = None
    try:
        con = duckdb_client or get_duckdb_connection(connection)
        df = con.execute(f"SELECT * FROM read_parquet('{s3_path}')").df()
        structured_logger.info(
            event="trip_details_load_succeeded",
            message="Trip details loaded successfully",
            metadata={"record_count": df.shape[0]},
        )
        return df
    except Exception as e:
        structured_logger.error(
            event="trip_details_load_failed",
            message=f"Error fetching trip details from storage: {e}",
            error_type=type(e).__name__,
            error_message=str(e),
            metadata={"path": s3_path},
        )
        raise ValueError(f"Error fetching trip_details from storage: {e}") from e
    finally:
        if con:
            con.close()
