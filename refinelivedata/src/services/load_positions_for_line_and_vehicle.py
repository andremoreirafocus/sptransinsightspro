import logging

from src.infra.db import fetch_data_from_db_as_df

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_positions_for_line_and_vehicle(config, year, month, day, linha_lt, veiculo_id):
    table_name = config["POSITIONS_TABLE_NAME"]
    sql = f"""select veiculo_ts, veiculo_id,
       distance_to_first_stop, distance_to_last_stop,
       is_circular, linha_sentido, 
	   lt_origem, lt_destino
       from  {table_name}
       where linha_lt = '{linha_lt}' and veiculo_id = '{veiculo_id}'
       order by veiculo_ts asc;"""
    logger.info(f"Loading position data for line: {linha_lt}, veiculo_id: {veiculo_id}")
    df_filtered_positions = fetch_data_from_db_as_df(config, sql)
    logger.info(f"Loaded position data for line: {linha_lt}, veiculo_id: {veiculo_id}")
    logger.info(df_filtered_positions.head(5))
    return df_filtered_positions
