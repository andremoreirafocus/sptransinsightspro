from transformlivedata.services.load_trip_details import (
    load_trip_details,
)
from dateutil import parser
from typing import Dict, Any, Tuple
import pandas as pd
import logging

""
# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


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


def build_transformation_result(
    positions_tuple_list,
    invalid_vehicle_ids,
    invalid_trips,
    distance_errors,
    vehicle_count_discrepancies_per_line,
    total_vehicles_processeed,
    invalid_vehicles,
    total_lines_processed,
    expected_vehicles,
) -> Dict[str, Any]:
    """
    Build the final transformation result from raw components.
    Combines all collected data into the final result dictionary.
    """
    columns = [
        "extracao_ts",
        "veiculo_id",
        "linha_lt",
        "linha_code",
        "linha_sentido",
        "lt_destino",
        "lt_origem",
        "veiculo_prefixo",
        "veiculo_acessivel",
        "veiculo_ts",
        "veiculo_lat",
        "veiculo_long",
        "is_circular",
        "first_stop_id",
        "first_stop_lat",
        "first_stop_lon",
        "last_stop_id",
        "last_stop_lat",
        "last_stop_lon",
        "distance_to_first_stop",
        "distance_to_last_stop",
    ]
    positions_df = pd.DataFrame(positions_tuple_list, columns=columns)
    metrics = {
        "total_vehicles_processed": total_vehicles_processeed + invalid_vehicles,
        "valid_vehicles": total_vehicles_processeed,
        "invalid_vehicles": invalid_vehicles,
        "expected_vehicles": expected_vehicles,
        "total_lines_processed": total_lines_processed,
    }
    issues = {
        "invalid_vehicle_ids": invalid_vehicle_ids,
        "invalid_trips": list(invalid_trips),
        "distance_calculation_errors": distance_errors,
        "vehicle_count_discrepancies_per_line": vehicle_count_discrepancies_per_line,
    }
    result = {
        "positions": positions_df,
        "metrics": metrics,
        "issues": issues,
        "quality_score": 0.0,
    }
    result["quality_score"] = calculate_quality_score(result)
    return result


def transform_positions(config, raw_positions):
    def get_record_from_raw(vehicle, line, metadata, trip_details, distance_errors):
        linha = line.get("c")
        sentido = line.get("sl")
        if trip_details is None:
            return None
        else:
            is_circular = trip_details.get("is_circular")
            first_stop_dist, first_dist_success = calculate_distance(
                float(vehicle.get("py")),
                float(vehicle.get("px")),
                float(trip_details.get("first_stop_lat", 0.0)),
                float(trip_details.get("first_stop_lon", 0.0)),
            )
            if not first_dist_success:
                distance_errors.append(
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
                distance_errors.append(
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
        return vehicle_record

    logger.info("Converting raw positions to positions table...")
    invalid_vehicle_ids = []
    invalid_trips = set()
    distance_errors = []
    vehicle_count_discrepancies_per_line = []
    payload = raw_positions.get("payload")
    metadata = raw_positions.get("metadata")
    if "hr" not in payload:
        logger.error("No 'hr' field found in raw positions data.")
        return None
    if "l" not in payload:
        logger.error("No 'l' field found in raw positions data.")
        return None
    logger.info("Preloading trip details from database...")
    trip_details_in_memory_table = load_trip_details(config)
    logger.info(
        f"Built trip details cache with {len(trip_details_in_memory_table)} entries"
    )
    logger.info("Starting transformation of position data...")
    positions_table = []
    total_vehicles_processeed = 0
    invalid_vehicles = 0
    total_lines_processed = 0
    for line in payload["l"]:
        total_lines_processed += 1
        linha = line.get("c")
        sentido = line.get("sl")
        trip_id = get_trip_id(linha, sentido)
        if trip_id in trip_details_in_memory_table:
            trip_details = trip_details_in_memory_table[trip_id]
        else:
            logger.error(f"Trip details not found for trip_id: {trip_id}")
            trip_details = None
        number_of_vehicles_per_line = 0
        expected_vehicles_per_line = int(line.get("qv", 0))
        for vehicle in line.get("vs", []):
            if trip_details is None:
                vehicle_id = vehicle.get("p")
                invalid_vehicle_ids.append(vehicle_id)
                if trip_id not in invalid_trips:
                    invalid_trips.add(trip_id)
                invalid_vehicles += 1
                logger.warning(
                    f"Skipping vehicle {vehicle_id} on invalid trip {trip_id}"
                )
                continue
            vehicle_record = get_record_from_raw(
                vehicle, line, metadata, trip_details, distance_errors
            )
            number_of_vehicles_per_line += 1
            total_vehicles_processeed += 1
            positions_table.append(vehicle_record)
            if total_vehicles_processeed % 1000 == 0:
                logger.info(f"Processed {total_vehicles_processeed} vehicles.")
        if number_of_vehicles_per_line != expected_vehicles_per_line:
            vehicle_count_discrepancies_per_line.append(
                {
                    "linha": linha,
                    "expected": expected_vehicles_per_line,
                    "actual": number_of_vehicles_per_line,
                }
            )
            logger.warning(
                f"Expected {expected_vehicles_per_line} vehicles for line {linha}, but found {number_of_vehicles_per_line}."
            )
    total_vehicles_expected = metadata.get("total_vehicles", 0)
    result = build_transformation_result(
        positions_table,
        invalid_vehicle_ids,
        invalid_trips,
        distance_errors,
        vehicle_count_discrepancies_per_line,
        total_vehicles_processeed,
        invalid_vehicles,
        total_lines_processed,
        total_vehicles_expected,
    )
    logger.info(f"Processed {total_vehicles_processeed} valid vehicles.")
    logger.info(f"Skipped {invalid_vehicles} invalid vehicles.")
    logger.warning(
        f"Total invalid trips: {len(result['issues']['invalid_trips'])} - {result['issues']['invalid_trips']}"
    )
    logger.warning(
        f"Total invalid vehicles ids: {len(result['issues']['invalid_vehicle_ids'])} - {result['issues']['invalid_vehicle_ids']}"
    )
    return result


def get_transformation_metrics_and_issues_report(results):
    list_head_size = 10
    lines = []
    if "metrics" in results:
        lines.extend(
            [
                "TRANSFORMATION PROCESSING METRICS QUALITY ASSESSMENT",
                "-" * 80,
                f"Total Vehicles Processed: {results['metrics']['total_vehicles_processed']}",
                f"Valid Vehicles: {results['metrics']['valid_vehicles']}",
                f"Invalid Vehicles: {results['metrics']['invalid_vehicles']}",
                f"Bus lines identified: {results['metrics']['total_lines_processed']}",
            ]
        )
    if "quality_score" in results:
        lines.append(f"Quality score: {results['quality_score']}%")
    if "issues" in results:
        issues = results["issues"]
        if (
            issues["invalid_trips"]
            or issues["distance_calculation_errors"]
            or issues["vehicle_count_discrepancies_per_line"]
        ):
            lines.append("")
            lines.append("Issues Detected:")
            if issues["invalid_trips"]:
                lines.append(
                    f"  • Invalid Trips: {len(issues['invalid_trips'])} - {issues['invalid_trips'][:list_head_size]}{'...' if len(issues['invalid_trips']) > list_head_size else ''}"
                )
            if issues["invalid_vehicle_ids"]:
                lines.append(
                    f"  • Invalid Vehicles: {len(issues['invalid_vehicle_ids'])} - {issues['invalid_vehicle_ids'][:list_head_size]}{'...' if len(issues['invalid_vehicle_ids']) > list_head_size else ''}"
                )
            if issues["distance_calculation_errors"]:
                lines.append(
                    f"  • Distance Calculation Errors: {len(issues['distance_calculation_errors'])}"
                )
            if issues["vehicle_count_discrepancies_per_line"]:
                lines.append(
                    f"  • Vehicle Count Discrepancies per line: {len(issues['vehicle_count_discrepancies_per_line'])}"
                )
        lines.append("")
    return "\n".join(lines)
