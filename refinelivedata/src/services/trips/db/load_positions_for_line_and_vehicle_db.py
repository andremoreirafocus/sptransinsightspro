from src.infra.db import fetch_data_from_db_as_df
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_positions_for_line_and_vehicle_last_3_hous_db(config, linha_lt, veiculo_id):
    """
    Loads position records for a specific line and vehicle within the last 3 hours.
    The time window is calculated dynamically by the database.
    """
    table_name = config["POSITIONS_TABLE_NAME"]

    # SQL query using a 3-hour relative interval
    sql = f"""
        SELECT 
            veiculo_ts, 
            linha_sentido, 
            distance_to_first_stop, 
            distance_to_last_stop, 
            is_circular, 
            lt_origem, 
            lt_destino
        FROM {table_name}
        WHERE linha_lt = '{linha_lt}' 
          AND veiculo_id = {veiculo_id}
          AND veiculo_ts >= NOW() - INTERVAL '3 hours'
        ORDER BY veiculo_ts ASC;
    """

    # Fetch data as a DataFrame using the existing utility
    df_raw = fetch_data_from_db_as_df(config, sql)

    # Convert results to a list of dictionaries
    position_records = df_raw.to_dict("records")

    return position_records

def load_positions_for_line_and_vehicle_old(config, linha_lt, veiculo_id):
    table_name = config["POSITIONS_TABLE_NAME"]
    sql = f"""
        SELECT veiculo_ts, linha_sentido, distance_to_first_stop, distance_to_last_stop, is_circular, lt_origem, lt_destino
        FROM {table_name}
        WHERE linha_lt = '{linha_lt}' AND veiculo_id = {veiculo_id}
        ORDER BY veiculo_ts ASC;
    """

    df_raw = fetch_data_from_db_as_df(config, sql)
    position_records = df_raw.to_dict("records")

    return position_records


# def load_positions_for_line_and_vehicle(config, year, month, day, linha_lt, veiculo_id):
#     """
#     Loads position records for a specific line, vehicle, and date.
#     Filters the veiculo_ts column by year, month, and day.
#     """
#     table_name = config["POSITIONS_TABLE_NAME"]

#     # Building the SQL with EXTRACT filters for the date components
#     sql = f"""
#         SELECT
#             veiculo_ts,
#             linha_sentido,
#             distance_to_first_stop,
#             distance_to_last_stop,
#             is_circular,
#             lt_origem,
#             lt_destino
#         FROM {table_name}
#         WHERE linha_lt = '{linha_lt}'
#           AND veiculo_id = {veiculo_id}
#           AND EXTRACT(YEAR FROM veiculo_ts) = {year}
#           AND EXTRACT(MONTH FROM veiculo_ts) = {month}
#           AND EXTRACT(DAY FROM veiculo_ts) = {day}
#         ORDER BY veiculo_ts ASC;
#     """

#     # Fetch data using your existing database utility
#     df_raw = fetch_data_from_db_as_df(config, sql)

#     # Convert the DataFrame to a list of dictionaries
#     position_records = df_raw.to_dict("records")

#     return position_records


