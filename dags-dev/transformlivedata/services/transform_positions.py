from transformlivedata.services.load_trip_details import load_trip_details
from dateutil import parser
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, List
import pandas as pd
import logging
from transformlivedata.lineage.lineage_functions import (
    get_json_raw_fields_path_from_schema,
    build_api_lineage,
    get_column_type,
    build_join_lineage,
    merge_lineage_fragments,
)

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def calculate_quality_score(result: Dict[str, Any]) -> float:
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

    return f"{linha}-{sentido_convertido(sentido)}"


def calculate_distance(lat1, lon1, lat2, lon2) -> Tuple[float, bool]:
    from math import radians, sin, cos, sqrt, atan2

    try:
        R = 6371000
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


def flatten_raw_positions(raw_positions: Dict[str, Any]) -> pd.DataFrame:
    payload_lines = raw_positions.get("payload", {}).get("l", [])
    df_flat = pd.json_normalize(
        payload_lines,
        record_path=["vs"],
        meta=["c", "cl", "sl", "lt0", "lt1", "qv"],
        sep="_",
        errors="ignore",
    )
    return df_flat


def normalize_columns(
    df_flat: pd.DataFrame,
    rename_map: Dict[str, str],
    raw_path_map: Dict[str, str],
    metadata: Dict[str, Any],
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    df = df_flat.rename(columns=rename_map)
    df["extracao_ts"] = parser.parse(metadata.get("extracted_at"))
    df["linha_sentido"] = df["linha_sentido"].astype("Int64")
    df["linha_code"] = df["linha_code"].astype("Int64")
    df["veiculo_id"] = df["veiculo_id"].astype("Int64")
    df["veiculo_ts"] = pd.to_datetime(df["veiculo_ts"], errors="coerce")
    df["veiculo_lat"] = pd.to_numeric(df["veiculo_lat"], errors="coerce")
    df["veiculo_long"] = pd.to_numeric(df["veiculo_long"], errors="coerce")
    lineage = build_api_lineage(df, rename_map, raw_path_map)
    lineage["extracao_ts"] = {
        "inputs": ["ingest_service"],
        "type": get_column_type(df, "extracao_ts"),
        "transformation": "ingest timestamp",
    }
    return df, lineage


def add_trip_id(df: pd.DataFrame) -> pd.DataFrame:
    df["trip_id"] = df.apply(
        lambda row: get_trip_id(row["linha_lt"], row["linha_sentido"]), axis=1
    )
    return df


def enrich_with_trip_details(
    df: pd.DataFrame, trip_details_df: pd.DataFrame
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if trip_details_df.empty:
        df["_merge"] = "left_only"
        return df, {}
    merge_key = "trip_id"
    merge_table_name = "trip_details"
    df_enriched = df.merge(trip_details_df, on=merge_key, how="left", indicator=True)
    lineage = build_join_lineage(
        df_enriched, merge_table_name, merge_key, trip_details_df.columns
    )
    return df_enriched, lineage


def compute_distances(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]], Dict[str, Any]]:
    distance_errors = []

    def calc_first(row):
        dist, ok = calculate_distance(
            float(row["veiculo_lat"]),
            float(row["veiculo_long"]),
            float(row["first_stop_lat"]),
            float(row["first_stop_lon"]),
        )
        if not ok:
            distance_errors.append(
                {
                    "vehicle_id": row["veiculo_id"],
                    "linha": row["linha_lt"],
                    "error_type": "first_stop_distance",
                }
            )
        return dist

    def calc_last(row):
        dist, ok = calculate_distance(
            float(row["veiculo_lat"]),
            float(row["veiculo_long"]),
            float(row["last_stop_lat"]),
            float(row["last_stop_lon"]),
        )
        if not ok:
            distance_errors.append(
                {
                    "vehicle_id": row["veiculo_id"],
                    "linha": row["linha_lt"],
                    "error_type": "last_stop_distance",
                }
            )
        return dist

    df["distance_to_first_stop"] = df.apply(calc_first, axis=1)
    df["distance_to_last_stop"] = df.apply(calc_last, axis=1)
    lineage = build_calc_lineage(df)
    return df, distance_errors, lineage


def split_valid_invalid(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    invalid_mask = df["_merge"] == "left_only"
    valid_df = df.loc[~invalid_mask].copy()
    invalid_df = df.loc[invalid_mask].copy()
    if not invalid_df.empty:
        invalid_df["invalid_reason"] = "transform_error:trip_details_missing"
        invalid_df["validation_failed_at"] = datetime.now(timezone.utc)
    return valid_df, invalid_df


def build_metrics_and_issues(
    raw_positions: Dict[str, Any],
    valid_df: pd.DataFrame,
    invalid_df: pd.DataFrame,
    distance_errors: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    payload_lines = raw_positions.get("payload", {}).get("l", [])
    total_lines_processed = len(payload_lines)
    expected_vehicles = raw_positions.get("metadata", {}).get("total_vehicles", 0)
    valid_count = valid_df.shape[0]
    invalid_count = invalid_df.shape[0]
    metrics = {
        "total_vehicles_processed": valid_count + invalid_count,
        "valid_vehicles": valid_count,
        "invalid_vehicles": invalid_count,
        "expected_vehicles": expected_vehicles,
        "total_lines_processed": total_lines_processed,
    }
    invalid_vehicle_ids = (
        invalid_df["veiculo_id"].dropna().astype(int).tolist()
        if not invalid_df.empty
        else []
    )
    invalid_trips = (
        invalid_df["trip_id"].dropna().astype(str).unique().tolist()
        if not invalid_df.empty
        else []
    )
    lines_with_invalid_vehicles = (
        int(invalid_df["linha_lt"].nunique()) if not invalid_df.empty else 0
    )
    issues = {
        "invalid_vehicle_ids": invalid_vehicle_ids,
        "invalid_trips": invalid_trips,
        "distance_calculation_errors": distance_errors,
        "lines_with_invalid_vehicles": lines_with_invalid_vehicles,
    }
    return metrics, issues


def build_transformation_result(
    valid_df: pd.DataFrame,
    invalid_df: pd.DataFrame,
    valid_df_columns: List[str],
    metrics: Dict[str, Any],
    issues: Dict[str, Any],
    batch_ts,
    lineage: Dict[str, Any],
) -> Dict[str, Any]:

    positions_df = (
        valid_df[valid_df_columns].copy()
        if not valid_df.empty
        else pd.DataFrame(columns=valid_df_columns)
    )
    invalid_df_columns = valid_df_columns + ["invalid_reason", "validation_failed_at"]
    if invalid_df.empty:
        invalid_positions_df = pd.DataFrame(columns=invalid_df_columns)
    else:
        invalid_positions_df = invalid_df[invalid_df_columns].copy()
    result = {
        "positions": positions_df,
        "invalid_positions": invalid_positions_df,
        "batch_ts": batch_ts,
        "metrics": metrics,
        "issues": issues,
        "lineage": lineage,
        "quality_score": 0.0,
    }
    result["quality_score"] = calculate_quality_score(result)
    return result


def transform_positions(config, raw_positions):
    def get_config(config):
        raw_schema = config.get("raw_data_json_schema")
        if not raw_schema:
            raise ValueError("raw_data_json_schema is required in config.")
        return raw_schema

    logger.info("Converting raw positions to positions table...")
    metadata = raw_positions.get("metadata")
    logger.info("Preloading trip details from database...")
    trip_details_df = load_trip_details(config["general"])
    if trip_details_df is None or trip_details_df.empty:
        trip_details_df = pd.DataFrame()
    logger.info(f"Built trip details cache with {trip_details_df.shape[0]} entries")
    df_flat = flatten_raw_positions(raw_positions)
    if df_flat.empty:
        logger.error("No position data found to transform.")
        return None
    rename_map = {
        "c": "linha_lt",
        "cl": "linha_code",
        "sl": "linha_sentido",
        "lt0": "lt_destino",
        "lt1": "lt_origem",
        "p": "veiculo_id",
        "a": "veiculo_acessivel",
        "ta": "veiculo_ts",
        "py": "veiculo_lat",
        "px": "veiculo_long",
    }
    raw_schema = get_config(config)
    raw_path_map = get_json_raw_fields_path_from_schema(raw_schema)
    df_normalized, lineage_api = normalize_columns(
        df_flat, rename_map, raw_path_map, metadata
    )
    df_with_trip_id = add_trip_id(df_normalized)
    df_enriched, lineage_join = enrich_with_trip_details(
        df_with_trip_id, trip_details_df
    )
    valid_df, invalid_df = split_valid_invalid(df_enriched)
    if not valid_df.empty:
        valid_df, distance_errors, lineage_calc = compute_distances(valid_df)
    else:
        distance_errors = []
        lineage_calc = {}
    invalid_df["distance_to_first_stop"] = None
    invalid_df["distance_to_last_stop"] = None
    metrics, issues = build_metrics_and_issues(
        raw_positions, valid_df, invalid_df, distance_errors
    )
    batch_ts = parser.parse(metadata.get("extracted_at"))
    valid_df_columns = [
        "extracao_ts",
        "veiculo_id",
        "linha_lt",
        "linha_code",
        "linha_sentido",
        "lt_destino",
        "lt_origem",
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
    lineage = merge_lineage_fragments(lineage_api, lineage_join, lineage_calc)
    result = build_transformation_result(
        valid_df,
        invalid_df,
        valid_df_columns,
        metrics,
        issues,
        batch_ts,
        lineage,
    )
    logger.info(f"Processed {metrics['valid_vehicles']} valid vehicles.")
    logger.info(f"Skipped {metrics['invalid_vehicles']} invalid vehicles.")
    logger.warning(
        f"Total invalid trips: {len(issues['invalid_trips'])} - {issues['invalid_trips']}"
    )
    logger.warning(
        f"Total invalid vehicles ids: {len(issues['invalid_vehicle_ids'])} - {issues['invalid_vehicle_ids']}"
    )
    return result


def build_calc_lineage(df: pd.DataFrame) -> Dict[str, Any]:
    return {
        "distance_to_first_stop": {
            "inputs": [
                "veiculo_lat",
                "veiculo_long",
                "first_stop_lat",
                "first_stop_lon",
            ],
            "type": get_column_type(df, "distance_to_first_stop"),
            "transformation": "calculated based on current position",
        },
        "distance_to_last_stop": {
            "inputs": [
                "veiculo_lat",
                "veiculo_long",
                "last_stop_lat",
                "last_stop_lon",
            ],
            "type": get_column_type(df, "distance_to_last_stop"),
            "transformation": "calculated based on current position",
        },
    }
