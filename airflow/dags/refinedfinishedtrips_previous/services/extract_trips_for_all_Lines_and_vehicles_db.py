from refinedfinishedtrips_previous.services.extract_trips_per_line_per_vehicle_db import (
    extract_trips_per_line_per_vehicle,
)
from refinedfinishedtrips_previous.infra.db import fetch_data_from_db_as_df
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def generate_trips_for_all_Lines_and_vehicles(config, all_lines_and_vehicles):
    year = "2026"
    month = "01"
    day = "15"

    total_records = len(all_lines_and_vehicles)
    num_processed = 0
    for record in all_lines_and_vehicles:
        logger.info(f"{record}")
        linha_lt = record["linha_lt"]
        veiculo_id = record["veiculo_id"]
        logger.info(f"Line: {linha_lt}, vehicle: {veiculo_id}")
        extract_trips_per_line_per_vehicle(
            config, year, month, day, linha_lt, veiculo_id
        )
        num_processed += 1
        logger.info(f"Record {num_processed}/{total_records} processed.")
    return total_records


def extract_trips_for_all_Lines_and_vehicles(config):
    # # date params below kept for compatibility during development
    # year = "2026"
    # month = "01"
    # day = "15"
    logger.info("Loading all lines and vehicles...")
    # all_lines_and_vehicles = load_all_lines_and_vehicles(config)
    all_lines_and_vehicles = load_all_lines_and_vehicles_last_3_hours(config)
    total_records = len(all_lines_and_vehicles)
    logger.info(f"Loaded {total_records} records for lines and vehicles")

    generate_trips_for_all_Lines_and_vehicles(config, all_lines_and_vehicles)

    # num_processed = 0
    # for record in all_lines_and_vehicles:
    #     logger.info(f"{record}")
    #     linha_lt = record["linha_lt"]
    #     veiculo_id = record["veiculo_id"]
    #     logger.info(f"Line: {linha_lt}, vehicle: {veiculo_id}")
    #     extract_trips_per_line_per_vehicle(
    #         config, year, month, day, linha_lt, veiculo_id
    #     )
    #     num_processed += 1
    #     logger.info(f"Record {num_processed}/{total_records} processed.")


def load_all_lines_and_vehicles_last_3_hours(config):
    """
    Retrieves a distinct list of all line/vehicle combinations
    that have reported positions within the last 3 hours.
    """
    table_name = config["POSITIONS_TABLE_NAME"]

    # SQL query with a 3-hour time filter and group by logic
    sql = f"""
        SELECT 
            linha_lt, 
            veiculo_id 
        FROM {table_name}
        WHERE veiculo_ts >= NOW() - INTERVAL '3 hours'
        GROUP BY linha_lt, veiculo_id
        ORDER BY linha_lt, veiculo_id;
    """

    # Fetch data using the existing utility function
    df_raw = fetch_data_from_db_as_df(config, sql)

    # Convert the unique combinations into a list of dictionaries
    position_records = df_raw.to_dict("records")

    return position_records


def load_all_lines_and_vehicles(config):
    table_name = config["POSITIONS_TABLE_NAME"]
    sql = f"""
        select linha_lt, veiculo_id from {table_name}
	    group by linha_lt, veiculo_id
	    order by linha_lt, veiculo_id
    """

    df_raw = fetch_data_from_db_as_df(config, sql)
    position_records = df_raw.to_dict("records")

    return position_records


def extract_trips_for_a_test_Line_and_vehicle(config):
    year = "2026"
    month = "01"
    day = "15"
    linha_lt = "2290-10"
    veiculo_id = "41539"
    extract_trips_per_line_per_vehicle(config, year, month, day, linha_lt, veiculo_id)
