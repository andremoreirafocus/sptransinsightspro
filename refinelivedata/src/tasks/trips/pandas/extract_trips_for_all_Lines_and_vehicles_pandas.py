from src.tasks.trips.pandas.extract_trips_per_line_per_vehicle_pandas import (
    extract_trips_per_line_per_vehicle,
)
from src.infra.db import fetch_data_from_db_as_df
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def extract_trips_for_all_Lines_and_vehicles_pandas(config):
    logger.info("Bulk loading last 3 hours of positions for all vehicles...")

    # 1. Fetch ALL data for the last 3 hours in one single operation
    table_name = config["POSITIONS_TABLE_NAME"]
    sql = f"""
        SELECT 
            veiculo_ts, linha_lt, veiculo_id, linha_sentido, 
            distance_to_first_stop, distance_to_last_stop, 
            is_circular, lt_origem, lt_destino
        FROM {table_name}
        WHERE veiculo_ts >= NOW() - INTERVAL '3 hours'
        ORDER BY veiculo_ts ASC;
    """
    df_all_positions = fetch_data_from_db_as_df(config, sql)

    if df_all_positions.empty:
        logger.warning("No position data found for the last 3 hours.")
        return

    # 2. Group by Line and Vehicle to process them efficiently
    grouped = df_all_positions.groupby(["linha_lt", "veiculo_id"])
    total_groups = len(grouped)
    logger.info(
        f"Processing {total_groups} unique line/vehicle combinations in memory."
    )

    num_processed = 0
    for (linha_lt, veiculo_id), df_group in grouped:
        # 3. Call the modified extraction function passing the pre-loaded data
        extract_trips_per_line_per_vehicle(config, linha_lt, veiculo_id, df_group)

        num_processed += 1
        if num_processed % 500 == 0:
            logger.info(f"Progress: {num_processed}/{total_groups} processed.")
