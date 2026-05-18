from infra.sql_db_v2 import update_db_table_with_dataframe
import logging

logger = logging.getLogger(__name__)


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
    logger.info(
        f"Updating table {trip_details_table_name} with {df_trip_details.shape[0]} records..."
    )
    try:
        save_fn(connection, df_trip_details, trip_details_table_name)
    except Exception as e:
        logger.error(
            "Failed to update table %s with %s records: %s",
            trip_details_table_name,
            df_trip_details.shape[0],
            e,
        )
        raise ValueError(
            f"Failed to update table {trip_details_table_name} "
            f"with {df_trip_details.shape[0]} records: {e}"
        ) from e
    logger.info(f"Updated table {trip_details_table_name} successfully!")
