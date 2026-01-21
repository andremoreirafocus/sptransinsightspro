from src.infra.db import fetch_data_from_db_as_df, execute_sql_command
import pandas as pd
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def get_trip_id(linha, sentido):
    def sentido_convertido(sentido):
        if sentido == 1:
            return 0
        elif sentido == 2:
            return 1
        else:
            return 999

    this_trip_id = f"{linha}-{sentido_convertido(sentido)}"
    return this_trip_id


def get_sentido_from_trip_id(trip_id):
    sentido_sem_conversao = int(trip_id.split()["1"])
    if sentido_sem_conversao == 0:
        return 1
    elif sentido_sem_conversao == 1:
        return 2
    else:
        return 999


def create_ongoing_trip(config, trip):
    print(trip)

    # Define the INSERT statement


def save_trip_to_ongoing(config, trip):
    """
    Formats the trip dictionary into a raw SQL string and executes
    it using execute_sql_command.
    """

    # Helper to format values for raw SQL string injection
    def format_sql_value(val):
        if val is None:
            return "NULL"
        if isinstance(val, bool):
            return "TRUE" if val else "FALSE"
        if isinstance(val, (int, float)):
            return str(val)
        # Wrap strings and timestamps in single quotes
        return f"'{str(val)}'"

    # print(f"trip: {trip}")
    # Build the raw SQL query
    sql = f"""
    INSERT INTO trusted.ongoing_trips (
        trip_id, 
        vehicle_id, 
        trip_start_time, 
        trip_end_time, 
        duration, 
        is_circular, 
        average_speed
    ) VALUES (
        {format_sql_value(trip["trip_id"])},
        {format_sql_value(trip["vehicle_id"])},
        {format_sql_value(trip["trip_start_time"])},
        {format_sql_value(trip["trip_end_time"])},
        {format_sql_value(trip["duration"])},
        {format_sql_value(trip["is_circular"])},
        {format_sql_value(trip["average_speed"])}
    );
    """
    # print(sql)
    # Call your specific execute function
    return sql


def update_ongoing_trip(trip_vehicle, position):
    pass


def delete_ongoing_trip():
    pass


def save_finished_trip(config, trip):
    print(trip)
    pass


def generate_trips_info_incrementally(config):
    positions = load_one_sample_positions(config)
    df_ongoing_trips = load_ongoing_trips(config)
    sqls = []

    for position in positions:
        linha = position["linha_lt"]
        sentido = position["linha_sentido"]
        vehicle_id = position["veiculo_id"]
        trip_id = get_trip_id(linha, sentido)
        ongoing_trips_not_empty = df_ongoing_trips.shape[0] > 0
        if ongoing_trips_not_empty:
            df_Filtered_trip = df_ongoing_trips[
                df_ongoing_trips["trip_id"]
                == trip_id & df_ongoing_trips["vehicle_id"]
                == vehicle_id
            ]
            ongping_trip_exists = df_Filtered_trip.shape[0] > 0
            if ongping_trip_exists:
                print(position)
                print(f"linha = {linha} sentido = {sentido} trip_id = {trip_id}")
                if sentido != get_sentido_from_trip_id(trip_id):
                    trip = {
                        "trip_id": trip_id,
                        "vehicle_id": vehicle_id,
                        "trip_start_time": df_Filtered_trip["trip_start_time"],
                        "trip_end_time": None,
                        "duration": None,
                        "is_circular": position["is_circular"],
                        "average_speed": 0.0,
                    }
                    save_finished_trip(trip)
                    delete_ongoing_trip(trip_id, vehicle_id)
                else:
                    update_ongoing_trip(trip_id, vehicle_id, position["veiculo_ts"])

        else:
            ongoing_trip = {
                "trip_id": trip_id,
                "vehicle_id": vehicle_id,
                "trip_start_time": position["veiculo_ts"],
                "trip_end_time": position["veiculo_ts"],
                "duration": None,
                "is_circular": position["is_circular"],
                "average_speed": 0.0,
            }
            sqls.append(save_trip_to_ongoing(config, ongoing_trip))

    execute_sql_command(config, sqls)


def load_one_sample_positions(config):
    table_name = config["LATEST_POSITIONS_TABLE_NAME"]
    sql = f"SELECT * FROM {table_name};"
    logger.info("Loading one shot positions table from database...")
    print(f"Pandas version: {pd.__version__}")
    df = fetch_data_from_db_as_df(config, sql)
    logger.info(f"Loaded trip details for {df.shape[0]} trips")
    position_records = df.to_dict("records")
    logger.info(f"Loaded position records for {len(position_records)} trips")
    return position_records


def load_ongoing_trips(config):
    table_name = config["ONGOING_TRIPS_TABLE_NAME"]
    sql = f"SELECT * FROM {table_name};"
    logger.info("Loading ongoing_trips table from database...")
    print(f"Pandas version: {pd.__version__}")
    df = fetch_data_from_db_as_df(config, sql)
    logger.info(f"Loaded trip details for {df.shape[0]} trips")
    return df
