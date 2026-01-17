import logging

from src.infra.db import fetch_data_from_db_as_df

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_positions_for_line_and_vehicle(config, year, month, day, linha_lt, veiculo_id):
    table_name = config["POSITIONS_TABLE_NAME"]
    sql = f"SELECT * FROM {table_name};"
    logger.info(f"Loading position data for line: {linha_lt}, veiculo_id: {veiculo_id}")
    filtered_positions = fetch_data_from_db_as_df(config, sql)
    logger.info(f"Loaded position data for line: {linha_lt}, veiculo_id: {veiculo_id}")
    logger.info(filtered_positions)
    return filtered_positions
