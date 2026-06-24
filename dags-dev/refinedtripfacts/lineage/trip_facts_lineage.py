from typing import Any, Callable, Dict, List

from sqlalchemy import create_engine, text

LINEAGE_DRIFT_WARNING = "lineage drift detected"


def get_trip_facts_output_columns() -> List[str]:
    return [
        "trip_id",
        "vehicle_id",
        "route_id",
        "direction",
        "started_at",
        "ended_at",
        "duration_seconds",
        "duration",
        "is_circular",
        "distance_meters",
        "avg_speed_kmh",
        "started_at_time_dim_key",
        "ended_at_time_dim_key",
        "logic_date",
        "created_at",
    ]


def get_trip_facts_lineage() -> Dict[str, Any]:
    column_map = {
        "trip_id": {
            "sources": ["finished_trips.trip_id"],
            "derivation": "copied",
            "expected_type": "text",
        },
        "vehicle_id": {
            "sources": ["finished_trips.vehicle_id"],
            "derivation": "copied",
            "expected_type": "integer",
        },
        "route_id": {
            "sources": ["finished_trips.trip_id"],
            "derivation": "left(trip_id, length(trip_id) - 2)",
            "expected_type": "text",
        },
        "direction": {
            "sources": ["finished_trips.trip_id"],
            "derivation": "CASE right(trip_id,1) WHEN '0' THEN 1 WHEN '1' THEN 2 END",
            "expected_type": "smallint",
        },
        "started_at": {
            "sources": ["finished_trips.trip_start_time"],
            "derivation": "renamed",
            "expected_type": "timestamp with time zone",
        },
        "ended_at": {
            "sources": ["finished_trips.trip_end_time"],
            "derivation": "renamed",
            "expected_type": "timestamp with time zone",
        },
        "duration_seconds": {
            "sources": ["finished_trips.duration_seconds"],
            "derivation": "copied",
            "expected_type": "integer",
        },
        "duration": {
            "sources": ["finished_trips.duration_seconds"],
            "derivation": "make_interval(secs => duration_seconds)",
            "expected_type": "interval",
        },
        "is_circular": {
            "sources": ["finished_trips.is_circular"],
            "derivation": "copied",
            "expected_type": "boolean",
        },
        "distance_meters": {
            "sources": ["finished_trips.distance_meters"],
            "derivation": "copied",
            "expected_type": "double precision",
        },
        "avg_speed_kmh": {
            "sources": ["finished_trips.avg_speed_kmh"],
            "derivation": "copied",
            "expected_type": "double precision",
        },
        "started_at_time_dim_key": {
            "sources": ["started_at"],
            "derivation": "to_char(started_at AT TIME ZONE 'America/Sao_Paulo','YYYYMMDDHH24')::int",
            "expected_type": "integer",
        },
        "ended_at_time_dim_key": {
            "sources": ["ended_at"],
            "derivation": "to_char(ended_at AT TIME ZONE 'America/Sao_Paulo','YYYYMMDDHH24')::int",
            "expected_type": "integer",
        },
        "logic_date": {
            "sources": ["finished_trips.logic_date"],
            "derivation": "copied",
            "expected_type": "timestamp with time zone",
        },
        "created_at": {
            "sources": [],
            "derivation": "column DEFAULT NOW()",
            "expected_type": "timestamp with time zone",
        },
    }
    return {
        "table_name": "trip_facts",
        "columns": column_map,
        "drift_detected": False,
        "warning": None,
    }


def get_trip_facts_table_columns(
    config: Dict[str, Any],
    engine_factory: Callable = create_engine,
) -> List[Dict[str, str]]:
    db = config["connections"]["database"]
    uri = (
        f"postgresql://{db['user']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['database']}"
    )
    engine = engine_factory(uri)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT column_name, data_type "
                    "FROM information_schema.columns "
                    "WHERE table_schema = 'refined' AND table_name = 'trip_facts' "
                    "ORDER BY column_name"
                )
            ).fetchall()
        return [{"column_name": r[0], "data_type": r[1]} for r in rows]
    finally:
        engine.dispose()


def validate_trip_facts_lineage(
    lineage: Dict[str, Any],
    actual_columns: List[Dict[str, str]],
) -> Dict[str, Any]:
    expected = lineage.get("columns", {})
    expected_names = sorted(expected.keys())
    actual_names = sorted(r["column_name"] for r in actual_columns)
    actual_type_map = {r["column_name"]: r["data_type"] for r in actual_columns}

    columns_added = sorted(set(actual_names) - set(expected_names))
    columns_removed = sorted(set(expected_names) - set(actual_names))
    columns_type_changed = [
        {
            "column": name,
            "expected_type": expected[name]["expected_type"],
            "actual_type": actual_type_map[name],
        }
        for name in sorted(set(expected_names) & set(actual_names))
        if expected[name]["expected_type"] != actual_type_map[name]
    ]

    drift_detected = bool(columns_added or columns_removed or columns_type_changed)
    lineage["validation"] = {
        "expected_columns": expected_names,
        "actual_columns": actual_names,
        "columns_added": columns_added,
        "columns_removed": columns_removed,
        "columns_type_changed": columns_type_changed,
    }
    lineage["drift_detected"] = drift_detected
    lineage["warning"] = LINEAGE_DRIFT_WARNING if drift_detected else None
    return lineage
