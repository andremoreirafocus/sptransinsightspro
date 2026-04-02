from infra.sql_db_v2 import update_db_table_with_dataframe
import logging

logger = logging.getLogger(__name__)


def save_trip_details_from_dataframe_to_refined(config, df_trip_details):
    def get_config(config):
        try:
            tables = config["general"]["tables"]
            database = config["general"]["database"]
            trip_details_table_name = tables["trip_details_table_name"]
            connection = {
                "host": database["host"],
                "port": database["port"],
                "database": database["database"],
                "user": database["user"],
                "password": database["password"],
            }
            return connection, trip_details_table_name
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    connection, trip_details_table_name = get_config(config)
    logger.info(
        f"Updating table {trip_details_table_name} with {df_trip_details.shape[0]} records..."
    )
    update_db_table_with_dataframe(connection, df_trip_details, trip_details_table_name)
    logger.info(f"Updated table {trip_details_table_name} successfully!")
