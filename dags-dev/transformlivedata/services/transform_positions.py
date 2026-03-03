from transformlivedata.services.load_trip_details_from_storage_to_dataframe import (
    load_trip_details_from_storage_to_dataframe,
)
from dateutil import parser
import logging
from typing import Dict, Any, List, Tuple

""
# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def create_transformation_result() -> Dict[str, Any]:
    """
    Create a transformation result dictionary to encapsulate results of position
    data transformation including the transformed data, metrics, and quality issues.
    """
    return {
        "positions_table": [],
        "metrics": {
            "total_vehicles_processed": 0,
            "valid_vehicles": 0,
            "invalid_vehicles": 0,
            "expected_vehicles": 0,
            "total_lines_processed": 0,
        },
        "issues": {
            "invalid_trips": [],
            "distance_calculation_errors": [],
            "vehicle_count_discrepancies": [],
        },
        "quality_score": 0.0,
    }


def calculate_quality_score(result: Dict[str, Any]) -> float:
    """
    Calculate quality score as percentage of valid vehicles.
    Score is 0-100.
    """
    if result["metrics"]["total_vehicles_processed"] == 0:
        return 0.0
    valid = result["metrics"]["valid_vehicles"]
    total = result["metrics"]["total_vehicles_processed"]
    return round((valid / total) * 100, 2)


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


def calculate_distance(lat1, lon1, lat2, lon2) -> Tuple[float, bool]:
    """
    Calculate the Haversine distance between two points on the Earth.
    :param lat1: Latitude of point 1
    :param lon1: Longitude of point 1
    :param lat2: Latitude of point 2
    :param lon2: Longitude of point 2
    :return: Tuple of (distance in meters, success_flag)
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
        return float(distance), True
    except Exception as e:
        logger.error(f"Error calculating distance: {e}")
        logger.error(f"lat1, lon1, lat2, lon2 = {lat1}, {lon1}, {lat2}, {lon2}")
        return -1.0, False


def transform_positions(config, raw_positions):
    def get_record_from_raw(vehicle, line, metadata, trip_details, result):
        # Use pre-computed trip_details instead of looking them up
        linha = line.get("c")
        sentido = line.get("sl")
        # trip_details is already computed and passed in
        is_circular = trip_details.get("is_circular")
        invalid_trip = trip_details.get("invalid_trip", False)
        if invalid_trip:
            first_stop_distance = 0.0  # Avoid zero distance
            last_stop_distance = 0.0
        else:
            # Calculate first stop distance
            first_stop_dist, first_dist_success = calculate_distance(
                float(vehicle.get("py")),
                float(vehicle.get("px")),
                float(trip_details.get("first_stop_lat", 0.0)),
                float(trip_details.get("first_stop_lon", 0.0)),
            )
            if not first_dist_success:
                result["issues"]["distance_calculation_errors"].append(
                    {
                        "vehicle_id": vehicle.get("p"),
                        "linha": linha,
                        "error_type": "first_stop_distance",
                    }
                )
            first_stop_distance = first_stop_dist

            # Calculate last stop distance
            last_stop_dist, last_dist_success = calculate_distance(
                float(vehicle.get("py")),
                float(vehicle.get("px")),
                float(trip_details.get("last_stop_lat", 0.0)),
                float(trip_details.get("last_stop_lon", 0.0)),
            )
            if not last_dist_success:
                result["issues"]["distance_calculation_errors"].append(
                    {
                        "vehicle_id": vehicle.get("p"),
                        "linha": linha,
                        "error_type": "last_stop_distance",
                    }
                )
            last_stop_distance = last_stop_dist

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

        return vehicle_record, invalid_trip

    logger.info("Converting raw positions to positions table...")
    result = create_transformation_result()

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
    df_trip_details = load_trip_details_from_storage_to_dataframe(config)

    # Build trip_details lookup dictionary for O(1) access
    trip_details_dict = {
        row["trip_id"]: row.to_dict() for _, row in df_trip_details.iterrows()
    }
    logger.info(f"Built trip details cache with {len(trip_details_dict)} entries")

    logger.info("Starting transformation of position data...")
    total_number_of_vehicles = 0
    invalid_vehicles = 0

    for line in payload["l"]:
        result["metrics"]["total_lines_processed"] += 1
        # Pre-compute trip details once per line, not per vehicle
        linha = line.get("c")
        sentido = line.get("sl")
        trip_id = get_trip_id(linha, sentido)

        # Use cached lookup instead of dataframe filtering
        if trip_id in trip_details_dict:
            trip_details = trip_details_dict[trip_id]
        else:
            # Return default values if not found
            logger.error(f"Trip details not found for trip_id: {trip_id}")
            trip_details = {
                "is_circular": False,
                "first_stop_id": 0,
                "first_stop_lat": 0.0,
                "first_stop_lon": 0.0,
                "last_stop_id": 0,
                "last_stop_lat": 0.0,
                "last_stop_lon": 0.0,
                "invalid_trip": True,
            }

        number_of_vehicles_per_line = 0
        expected_vehicles_per_line = int(line.get("qv", 0))

        for vehicle in line.get("vs", []):
            # Pass result to collect issues
            vehicle_record, invalid_trip = get_record_from_raw(
                vehicle, line, metadata, trip_details, result
            )
            if invalid_trip:
                this_trip_id = get_trip_id(linha, sentido)
                if this_trip_id not in result["issues"]["invalid_trips"]:
                    result["issues"]["invalid_trips"].append(this_trip_id)
                    logger.warning(
                        f"Skipping vehicle {vehicle.get('p')} on invalid trip {this_trip_id}"
                    )
                invalid_vehicles += 1
                continue

            number_of_vehicles_per_line += 1
            total_number_of_vehicles += 1
            result["positions_table"].append(vehicle_record)

            if total_number_of_vehicles % 1000 == 0:
                logger.info(f"Processed {total_number_of_vehicles} vehicles.")

        # Check for vehicle count discrepancies
        if number_of_vehicles_per_line != expected_vehicles_per_line:
            result["issues"]["vehicle_count_discrepancies"].append(
                {
                    "linha": linha,
                    "expected": expected_vehicles_per_line,
                    "actual": number_of_vehicles_per_line,
                }
            )
            logger.warning(
                f"Expected {expected_vehicles_per_line} vehicles for line {linha}, but found {number_of_vehicles_per_line}."
            )

    # Update metrics
    result["metrics"]["total_vehicles_processed"] = (
        total_number_of_vehicles + invalid_vehicles
    )
    result["metrics"]["valid_vehicles"] = total_number_of_vehicles
    result["metrics"]["invalid_vehicles"] = invalid_vehicles
    result["metrics"]["expected_vehicles"] = metadata.get("total_vehicles", 0)

    # Calculate and set quality score
    result["quality_score"] = calculate_quality_score(result)

    logger.info(f"Processed {total_number_of_vehicles} valid vehicles.")
    logger.info(f"Skipped {invalid_vehicles} invalid vehicles.")
    logger.warning(
        f"Total invalid trips: {len(result['issues']['invalid_trips'])} - {result['issues']['invalid_trips']}"
    )

    return result


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
