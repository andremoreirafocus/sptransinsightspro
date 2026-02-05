from transformdatatotrusted.infra.db import fetch_data_from_db_as_df
from dateutil import parser
import logging
import pandas as pd

""
# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_trip_details_to_dataframe(config):
    """
    Loads the trusted.trip_details table from the database
    and returns a Pandas DataFrame for analysis.
    """
    table_name = config["TRIP_DETAILS_TABLE_NAME"]
    sql = f"SELECT * FROM {table_name};"
    logger.info("Loading trip_details table from database...")

    # This is the standard way to check
    print(f"Pandas version: {pd.__version__}")
    df = fetch_data_from_db_as_df(config, sql)
    logger.info(f"Loaded trip details for {df.shape[0]} trips")
    return df


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


def get_trip_details(df, linha, sentido):
    this_trip_id = get_trip_id(linha, sentido)
    # print(f"Fetching trip details for trip_id: {this_trip_id}")
    df_details = df[df["trip_id"] == this_trip_id]

    if df_details.shape[0] > 0:
        return df_details.to_dict(orient="records")[0]
    else:
        # print(f"Trip details not found for trip_id: {this_trip_id}")
        logger.error(f"Trip details not found for trip_id: {this_trip_id}")
        # print(df_details.head(5))
        # exit()
        return {
            "is_circular": False,
            "first_stop_id": 0,
            "first_stop_lat": 0.0,
            "first_stop_lon": 0.0,
            "last_stop_id": 0,
            "last_stop_lat": 0.0,
            "last_stop_lon": 0.0,
            "invalid_trip": True,
        }


def calculate_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the Haversine distance between two points on the Earth.
    :param lat1: Latitude of point 1
    :param lon1: Longitude of point 1
    :param lat2: Latitude of point 2
    :param lon2: Longitude of point 2
    :return: Distance in meters
    """
    from math import radians, sin, cos, sqrt, atan2

    try:
        R = 6371000  # Radius of the Earth in meters
        phi1 = radians(lat1)
        phi2 = radians(lat2)
        delta_phi = radians(lat2 - lat1)
        delta_lambda = radians(lon2 - lon1)

        a = sin(delta_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(delta_lambda / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        distance = round(R * c)
        return distance
    except Exception as e:
        logger.error(f"Error calculating distance: {e}")
        # print(f"Error calculating distance: {e}")
        logger.error(f"lat1, lon1, lat2, lon2 = {lat1}, {lon1}, {lat2}, {lon2}")
        # print(type(lat1), type(lon1), type(lat2), type(lon2))
        return -1


def transform_positions(config, raw_positions):
    def get_record_from_raw(vehicle, line, metadata, df_trip_details):
        linha = line.get("c")
        sentido = line.get("sl")
        trip_details = get_trip_details(df_trip_details, linha, sentido)
        # print(f"Trip details: {trip_details}")
        is_circular = trip_details.get("is_circular")
        # print(f"is_circular: {is_circular}")
        invalid_trip = trip_details.get("invalid_trip", False)
        if invalid_trip:
            first_stop_distance = 0.0  # Avoid zero distance
            last_stop_distance = 0.0
        else:
            # print("Calculating first_stop_distance...")
            first_stop_distance = float(
                calculate_distance(
                    float(vehicle.get("py")),
                    float(vehicle.get("px")),
                    float(trip_details.get("first_stop_lat", 0.0)),
                    float(trip_details.get("first_stop_lon", 0.0)),
                ),
            )
            # print("Calculating last_stop_distance...")
            last_stop_distance = float(
                calculate_distance(
                    float(vehicle.get("py")),
                    float(vehicle.get("px")),
                    float(trip_details.get("last_stop_lat", 0.0)),
                    float(trip_details.get("last_stop_lon", 0.0)),
                ),
            )
        vehicle_record = (
            parser.parse(metadata.get("extracted_at")),  # extracao_ts
            int(vehicle.get("p")),  # veiculo_id
            linha,  # linha_lt
            int(line.get("cl")),  # linha_code
            int(sentido),  # linha_sentido
            line.get("lt0"),  # lt_destino
            line.get("lt1"),  # lt_origem
            int(vehicle.get("p")),  # veiculo_prefixo
            bool(vehicle.get("a")),  # veiculo_acessivel
            parser.parse(vehicle.get("ta")),  # veiculo_ts
            float(vehicle.get("py")),  # veiculo_lat
            float(vehicle.get("px")),  # veiculo_long
            is_circular,
            trip_details.get("first_stop_id"),
            trip_details.get("first_stop_lat"),
            trip_details.get("first_stop_lon"),
            trip_details.get("last_stop_id"),
            trip_details.get("last_stop_lat"),
            trip_details.get("last_stop_lon"),
            first_stop_distance,
            last_stop_distance,
        )
        if last_stop_distance == -1 or first_stop_distance == -1:
            logger.error(
                f"Invalid distance calculation for vehicle {vehicle.get('p')} on line {linha}"
            )
            # print(
            #     f"Invalid distance calculation for vehicle {vehicle.get('p')} on line {linha}"
            # )
            # print(f"\nvehicle_record: {vehicle_record}\n")
            # print(vehicle)
            logger.info(f"Trip details: {trip_details}")

        return vehicle_record, invalid_trip

    logger.info("Converting raw positions to positions table...")
    positions_table = []
    list_of_invalid_trips = []
    if not data_structure_is_valid(raw_positions):
        logger.error("Raw positions data structure is invalid.")
        return None
    payload = raw_positions.get("payload")
    metadata = raw_positions.get("metadata")
    if "hr" not in payload:
        logger.error("No 'hr' field found in raw positions data.")
        return None
    if "l" not in payload:
        logger.error("No 'l' field found in raw positions data.")
        return None
    logger.info("Preloading trip details from database...")
    df_trip_details = load_trip_details_to_dataframe(config)
    # print(df_trip_details)
    logger.info("Starting transformation of position data...")
    total_number_of_vehicles = 0
    for line in payload["l"]:
        number_of_vehicles_per_line = 0
        for vehicle in line.get("vs", []):
            vehicle_record, invalid_trip = get_record_from_raw(
                vehicle, line, metadata, df_trip_details
            )
            if invalid_trip:
                linha = line.get("c")
                sentido = line.get("sl")
                this_trip_id = get_trip_id(linha, sentido)
                if this_trip_id not in list_of_invalid_trips:
                    list_of_invalid_trips.append(this_trip_id)
                    logger.warning(
                        f"Skipping vehicle {vehicle.get('p')} on invalid trip {this_trip_id}"
                    )
                continue
            number_of_vehicles_per_line += 1
            total_number_of_vehicles += 1
            positions_table.append(vehicle_record)
            if total_number_of_vehicles % 1000 == 0:
                logger.info(f"Processed {total_number_of_vehicles} vehicles.")
        if number_of_vehicles_per_line != int(line.get("qv")):
            logger.warning(
                f"Expected {line.get('q', 0)} vehicles for line {line.get('qv')}, but found {number_of_vehicles_per_line}."
            )
    logger.info(f"Processed {total_number_of_vehicles} vehicles.")
    list_of_invalid_trips.sort()
    logger.warning(
        f"Total invalid trips skipped: {len(list_of_invalid_trips)} - {list_of_invalid_trips}"
    )
    return positions_table


def data_structure_is_valid(data):
    """
    Validate the structure of the incoming data.
    :param data: The data to validate
    :return: True if valid, False otherwise
    """
    if not isinstance(data, dict):
        logger.error("Data does not have a valid structure.")
        return False
    required_fields = ["payload", "metadata"]
    for field in required_fields:
        if field not in data:
            logger.error(f"Missing required field: {field}")
            logger.error(f"Data content: {data}")
            return False
    if not isinstance(data.get("metadata"), dict):
        logger.error("Data metadata does not have a valid structure.")
        return False
    required_fields = ["source", "extracted_at", "total_vehicles"]
    for field in required_fields:
        if field not in data.get("metadata"):
            logger.error(f"Missing required metadata field: {field}")
            logger.error(f"Metadata content: {data.get('metadata')}")
            return False
    if not isinstance(data.get("payload"), dict):
        logger.error("Data payload does not have a valid structure.")
        logger.error(f"Payload content: {data.get('payload')}")
        logger.error(f"Metadata content: {data.get('metadata')}")
        return False
    required_fields = ["hr", "l"]
    for field in required_fields:
        if field not in data.get("payload"):
            logger.error(f"Missing required payload field: {field}")
            return False
    return True
