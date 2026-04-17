from typing import Any, Dict, Iterable


LINEAGE_DRIFT_WARNING = "lineage drift detected"


def get_trip_details_lineage() -> Dict[str, Any]:
    column_map = {
        "trip_id": {
            "sources": ["stop_times.trip_id"],
            "derivation": "base trip id; missing '-1' patched from '-0' via replace",
        },
        "first_stop_id": {
            "sources": ["stop_times.stop_id", "stop_times.stop_sequence", "stops.stop_id"],
            "derivation": "first stop by min(stop_sequence); swapped with last for non-circular missing patch",
        },
        "first_stop_name": {
            "sources": ["stops.stop_name", "stop_times.stop_sequence"],
            "derivation": "name from first stop id; swapped with last for non-circular missing patch",
        },
        "first_stop_lat": {
            "sources": ["stops.stop_lat", "stop_times.stop_sequence"],
            "derivation": "lat from first stop id; swapped with last for non-circular missing patch",
        },
        "first_stop_lon": {
            "sources": ["stops.stop_lon", "stop_times.stop_sequence"],
            "derivation": "lon from first stop id; swapped with last for non-circular missing patch",
        },
        "last_stop_id": {
            "sources": ["stop_times.stop_id", "stop_times.stop_sequence", "stops.stop_id"],
            "derivation": "last stop by max(stop_sequence); swapped with first for non-circular missing patch",
        },
        "last_stop_name": {
            "sources": ["stops.stop_name", "stop_times.stop_sequence"],
            "derivation": "name from last stop id; swapped with first for non-circular missing patch",
        },
        "last_stop_lat": {
            "sources": ["stops.stop_lat", "stop_times.stop_sequence"],
            "derivation": "lat from last stop id; swapped with first for non-circular missing patch",
        },
        "last_stop_lon": {
            "sources": ["stops.stop_lon", "stop_times.stop_sequence"],
            "derivation": "lon from last stop id; swapped with first for non-circular missing patch",
        },
        "trip_linear_distance": {
            "sources": ["first_stop_lat", "first_stop_lon", "last_stop_lat", "last_stop_lon"],
            "derivation": "round(sqrt((last_lat-first_lat)^2 + (last_lon-first_lon)^2) * 106428)",
        },
        "is_circular": {
            "sources": ["first_stop_id", "last_stop_id"],
            "derivation": "first_stop_id == last_stop_id",
        },
    }
    return {
        "table_name": "trip_details",
        "columns": column_map,
        "drift_detected": False,
        "warning": None,
    }


def validate_trip_details_lineage(
    lineage: Dict[str, Any],
    dataframe_columns: Iterable[str],
) -> Dict[str, Any]:
    expected_columns = sorted(lineage.get("columns", {}).keys())
    actual_columns = sorted([str(column) for column in dataframe_columns])
    missing_in_lineage = sorted(set(actual_columns) - set(expected_columns))
    missing_in_dataframe = sorted(set(expected_columns) - set(actual_columns))
    drift_detected = len(missing_in_lineage) > 0 or len(missing_in_dataframe) > 0
    lineage["validation"] = {
        "expected_columns": expected_columns,
        "actual_columns": actual_columns,
        "missing_in_lineage": missing_in_lineage,
        "missing_in_dataframe": missing_in_dataframe,
    }
    lineage["drift_detected"] = drift_detected
    lineage["warning"] = LINEAGE_DRIFT_WARNING if drift_detected else None
    return lineage
