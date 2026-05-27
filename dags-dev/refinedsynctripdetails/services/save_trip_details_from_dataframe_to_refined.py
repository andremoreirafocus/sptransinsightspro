from infra.sql_db_v2 import update_db_table_with_dataframe
from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)


def save_trip_details_from_dataframe_to_refined(
    config, df_trip_details, save_fn=update_db_table_with_dataframe
):
    def get_config(config):
        tables = config["general"]["tables"]
        database = config["connections"]["database"]
        trip_details_table_name = tables["trip_details_table_name"]
        connection = {
            "host": database["host"],
            "port": database["port"],
            "database": database["database"],
            "user": database["user"],
            "password": database["password"],
        }
        return connection, trip_details_table_name

    connection, trip_details_table_name = get_config(config)
    structured_logger.info(
        event="trip_details_save_started",
        message=f"Saving trip details to table '{trip_details_table_name}'",
        metadata={"table": trip_details_table_name, "record_count": df_trip_details.shape[0]},
    )
    try:
        save_fn(connection, df_trip_details, trip_details_table_name)
    except Exception as e:
        structured_logger.error(
            event="trip_details_save_failed",
            message=f"Failed to update table '{trip_details_table_name}': {e}",
            error_type=type(e).__name__,
            error_message=str(e),
            metadata={"table": trip_details_table_name},
        )
        raise ValueError(
            f"Failed to update table {trip_details_table_name} "
            f"with {df_trip_details.shape[0]} records: {e}"
        ) from e
    structured_logger.info(
        event="trip_details_save_succeeded",
        message=f"Table '{trip_details_table_name}' updated successfully",
        status="SUCCEEDED",
        metadata={"table": trip_details_table_name, "record_count": df_trip_details.shape[0]},
    )
