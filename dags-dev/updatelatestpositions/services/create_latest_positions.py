from infra.duck_db_v2 import get_duckdb_connection
from infra.sql_db_v2 import update_db_table_with_dataframe
from updatelatestpositions.services.get_latest_path_for_query import (
    get_latest_path_for_query,
)
import logging

logger = logging.getLogger(__name__)


def create_latest_positions_table(config):
    def get_config(config):
        try:
            general = config["general"]
            storage = general["storage"]
            tables = general["tables"]
            database = general["database"]
            latest_positions_table_name = tables["latest_positions_table_name"]
            storage_connection = {
                "minio_endpoint": storage["minio_endpoint"],
                "access_key": storage["access_key"],
                "secret_key": storage["secret_key"],
                "secure": False,
            }
            database_connection = {
                "host": database["host"],
                "port": database["port"],
                "database": database["database"],
                "user": database["user"],
                "password": database["password"],
            }
            return latest_positions_table_name, storage_connection, database_connection
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    latest_positions_table_name, storage_connection, database_connection = get_config(
        config
    )
    try:
        latest_path_for_query = get_latest_path_for_query(config)
        if not latest_path_for_query:
            logger.error(
                "No recent data found in the last 2 hours. Scan aborted."
            )
            return
        logger.info(f"Discovery successful: {latest_path_for_query}")
        con = get_duckdb_connection(storage_connection)
        refined_df = con.execute(f"""
            SELECT veiculo_ts, veiculo_id, veiculo_lat, veiculo_long, linha_lt, linha_sentido,
                   linha_lt || '-' || (CASE WHEN linha_sentido = 1 THEN '0' 
                                            WHEN linha_sentido = 2 THEN '1' ELSE NULL END) AS trip_id
            FROM read_parquet('{latest_path_for_query}')
        """).df()
        total_records = refined_df.shape[0]
        logger.info(
            f"Updating table {latest_positions_table_name} with {total_records} records..."
        )
        update_db_table_with_dataframe(
            database_connection, refined_df, latest_positions_table_name
        )
        logger.info(f"Updated table {latest_positions_table_name} successfully!")

    except Exception as e:
        logger.error(f"Update failed: {e}")
        raise
    finally:
        if "con" in locals():
            con.close()
