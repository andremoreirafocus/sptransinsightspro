import json
import logging
from typing import Any, Dict, List

import pandas as pd

logger = logging.getLogger(__name__)


def _effective_window_minutes(df: pd.DataFrame) -> float:
    max_ts = df["extracao_ts"].max()
    min_ts = df["extracao_ts"].min()
    return round((max_ts - min_ts).total_seconds() / 60, 2)


def evaluate_zero_trips(config: Dict[str, Any], effective_window_minutes: float, trips_count: int) -> Dict[str, Any]:
    def get_config(config):
        return config["general"]["quality"]["trips_effective_window_threshold_minutes"]

    window_threshold = get_config(config)
    result = {
        "effective_window_minutes": effective_window_minutes,
        "window_threshold_minutes": window_threshold,
        "trips_count": trips_count,
    }
    logger.info(json.dumps({"event": "zero_trips_evaluation", "message": "Zero trips evaluation", "metadata": result}))
    return result


def evaluate_low_trip_count(config: Dict[str, Any], effective_window_minutes: float, trips_count: int) -> Dict[str, Any]:
    def get_config(config):
        quality = config["general"]["quality"]
        return (
            quality["trips_effective_window_threshold_minutes"],
            quality["trips_min_trips_threshold"],
        )

    window_threshold, min_trips_threshold = get_config(config)
    result = {
        "effective_window_minutes": effective_window_minutes,
        "window_threshold_minutes": window_threshold,
        "min_trips_threshold": min_trips_threshold,
        "trips_count": trips_count,
    }
    logger.info(json.dumps({"event": "low_trip_count_evaluation", "message": "Low trip count evaluation", "metadata": result}))
    return result


def validate_trips_quality(
    config: Dict[str, Any],
    df: pd.DataFrame,
    trips_extracted: List,
    extraction_metrics: Dict[str, int],
) -> Dict[str, Any]:
    trips_count = len(trips_extracted)
    effective_window = _effective_window_minutes(df)

    zero_trips = evaluate_zero_trips(config, effective_window, trips_count)
    low_trip_count = evaluate_low_trip_count(config, effective_window, trips_count)

    checks = []

    if zero_trips["effective_window_minutes"] > zero_trips["window_threshold_minutes"] and zero_trips["trips_count"] == 0:
        checks.append({
            "check": "zero_trips",
            "status": "WARN",
            "reason": f"no trips extracted despite effective window of {effective_window} min (threshold: {zero_trips['window_threshold_minutes']} min)",
            **zero_trips,
        })
    else:
        checks.append({"check": "zero_trips", "status": "PASS", **zero_trips})

    if low_trip_count["effective_window_minutes"] > low_trip_count["window_threshold_minutes"] and low_trip_count["trips_count"] < low_trip_count["min_trips_threshold"]:
        checks.append({
            "check": "low_trip_count",
            "status": "WARN",
            "reason": f"only {trips_count} trips extracted (min threshold: {low_trip_count['min_trips_threshold']}) in {effective_window} min window",
            **low_trip_count,
        })
    else:
        checks.append({"check": "low_trip_count", "status": "PASS", **low_trip_count})

    overall = "WARN" if any(c["status"] == "WARN" for c in checks) else "PASS"
    result = {
        "status": overall,
        "effective_window_minutes": effective_window,
        "trips_extracted": trips_count,
        "source_sentido_discrepancies": extraction_metrics.get("total_source_sentido_discrepancies", 0),
        "sanitization_dropped_points": extraction_metrics.get("total_input_position_sanitization_drops", 0),
        "input_position_records": extraction_metrics.get("total_input_position_records", len(df)),
        "vehicle_line_groups_processed": extraction_metrics.get("vehicle_line_groups_processed", 0),
        "checks": checks,
    }
    logger.info(json.dumps({"event": "trips_quality_validated", "message": "Trips quality validation", "metadata": result}))
    return result
