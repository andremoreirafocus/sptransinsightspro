from typing import Any, Dict, Iterable, List


LINEAGE_DRIFT_WARNING = "lineage drift detected"


def get_finished_trips_output_columns() -> List[str]:
    return [
        "trip_id",
        "vehicle_id",
        "trip_start_time",
        "trip_end_time",
        "duration",
        "is_circular",
        "average_speed",
    ]


def get_finished_trips_lineage() -> Dict[str, Any]:
    column_map = {
        "trip_id": {
            "sources": ["linha_lt", "sentido"],
            "derivation": "built by get_trip_id(linha_lt, sentido)",
        },
        "vehicle_id": {
            "sources": ["veiculo_id"],
            "derivation": "copied from grouped vehicle key and cast to int",
        },
        "trip_start_time": {
            "sources": ["veiculo_ts", "start_position_index"],
            "derivation": 'position_records[start_position_index]["veiculo_ts"]',
        },
        "trip_end_time": {
            "sources": ["veiculo_ts", "end_position_index"],
            "derivation": 'position_records[end_position_index]["veiculo_ts"]',
        },
        "duration": {
            "sources": ["trip_start_time", "trip_end_time"],
            "derivation": "trip_end_time - trip_start_time",
        },
        "is_circular": {
            "sources": ["is_circular"],
            "derivation": 'copied from position_records[0]["is_circular"]',
        },
        "average_speed": {
            "sources": [],
            "derivation": "constant 0.0 placeholder in current implementation",
        },
    }
    return {
        "table_name": "finished_trips",
        "columns": column_map,
        "drift_detected": False,
        "warning": None,
    }


def validate_finished_trips_lineage(
    lineage: Dict[str, Any],
    dataframe_columns: Iterable[str],
) -> Dict[str, Any]:
    expected_columns = sorted(lineage.get("columns", {}).keys())
    actual_columns = sorted(str(column) for column in dataframe_columns)
    missing_in_lineage = sorted(set(actual_columns) - set(expected_columns))
    missing_in_dataframe = sorted(set(expected_columns) - set(actual_columns))
    drift_detected = bool(missing_in_lineage or missing_in_dataframe)
    lineage["validation"] = {
        "expected_columns": expected_columns,
        "actual_columns": actual_columns,
        "missing_in_lineage": missing_in_lineage,
        "missing_in_dataframe": missing_in_dataframe,
    }
    lineage["drift_detected"] = drift_detected
    lineage["warning"] = LINEAGE_DRIFT_WARNING if drift_detected else None
    return lineage
