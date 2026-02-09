from infra.duck_db import get_duckdb_connection
from infra.sql_db import update_db_table_with_dataframe
from updatelatestpositions.services.get_latest_path_for_query import (
    get_latest_path_for_query,
)
import logging

logger = logging.getLogger(__name__)


def create_latest_positions_table(config):
    def get_config(config):
        try:
            latest_positions_table_name = config["LATEST_POSITIONS_TABLE_NAME"]
            return (latest_positions_table_name,)
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    latest_positions_table_name = get_config(config)
    try:
        latest_path_for_query = get_latest_path_for_query(config)
        if not latest_path_for_query:
            logger.error(
                "No recent data found in the last 6 hours. Cost-saving scan aborted."
            )
            return
        logger.info(f"Discovery successful: {latest_path_for_query}")
        con = get_duckdb_connection(config)
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
        update_db_table_with_dataframe(config, refined_df)
        logger.info(f"Updated table {latest_positions_table_name} successfully!")

    except Exception as e:
        logger.error(f"Update failed: {e}")
        raise
    finally:
        if "con" in locals():
            con.close()
