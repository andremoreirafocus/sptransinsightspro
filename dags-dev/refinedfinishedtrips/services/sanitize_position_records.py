from datetime import datetime
from math import atan2, cos, radians, sin, sqrt
from typing import Any, Dict, List, Set, Tuple


def sanitize_position_records(
    position_records: List[Dict[str, Any]],
    max_reasonable_speed_kmh: float = 120.0,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if len(position_records) < 3:
        return position_records, {"dropped_points_count": 0, "violations": []}

    dropped_indices: Set[int] = set()
    violations: List[Dict[str, Any]] = []

    for current_index in range(1, len(position_records) - 1):
        if (current_index - 1) in dropped_indices:
            continue

        prev_record = position_records[current_index - 1]
        current_record = position_records[current_index]
        next_record = position_records[current_index + 1]

        if not _records_support_spatial_validation(
            prev_record, current_record, next_record
        ):
            continue

        prev_to_current_speed = _calculate_speed_kmh(prev_record, current_record)
        current_to_next_speed = _calculate_speed_kmh(current_record, next_record)
        prev_to_next_speed = _calculate_speed_kmh(prev_record, next_record)

        if (
            prev_to_current_speed is None
            or current_to_next_speed is None
            or prev_to_next_speed is None
        ):
            continue

        if (
            prev_to_current_speed > max_reasonable_speed_kmh
            and current_to_next_speed > max_reasonable_speed_kmh
            and prev_to_next_speed <= max_reasonable_speed_kmh
        ):
            dropped_indices.add(current_index)
            violations.append(
                {
                    "dropped_index": current_index,
                    "veiculo_ts": current_record["veiculo_ts"],
                    "prev_to_current_speed_kmh": round(prev_to_current_speed, 2),
                    "current_to_next_speed_kmh": round(current_to_next_speed, 2),
                    "prev_to_next_speed_kmh": round(prev_to_next_speed, 2),
                }
            )

    cleaned_records = [
        record
        for index, record in enumerate(position_records)
        if index not in dropped_indices
    ]
    return cleaned_records, {
        "dropped_points_count": len(dropped_indices),
        "violations": violations,
    }


def _records_support_spatial_validation(*records: Dict[str, Any]) -> bool:
    required_keys = ("veiculo_ts", "veiculo_lat", "veiculo_long")
    for record in records:
        for key in required_keys:
            if key not in record or record[key] is None:
                return False
    return True


def _calculate_speed_kmh(
    first_record: Dict[str, Any], second_record: Dict[str, Any]
) -> float | None:
    first_timestamp = _as_datetime(first_record["veiculo_ts"])
    second_timestamp = _as_datetime(second_record["veiculo_ts"])
    elapsed_seconds = (second_timestamp - first_timestamp).total_seconds()
    if elapsed_seconds <= 0:
        return None

    distance_meters = _calculate_distance_meters(
        float(first_record["veiculo_lat"]),
        float(first_record["veiculo_long"]),
        float(second_record["veiculo_lat"]),
        float(second_record["veiculo_long"]),
    )
    return (distance_meters / elapsed_seconds) * 3.6


def _calculate_distance_meters(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float:
    earth_radius_meters = 6371000
    phi1 = radians(lat1)
    phi2 = radians(lat2)
    delta_phi = radians(lat2 - lat1)
    delta_lambda = radians(lon2 - lon1)
    a = sin(delta_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(delta_lambda / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return earth_radius_meters * c


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
