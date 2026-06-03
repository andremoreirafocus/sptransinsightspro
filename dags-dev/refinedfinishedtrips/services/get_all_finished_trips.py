from typing import Any, Dict, List, Optional, Tuple, cast

import pandas as pd

from observability.structured_event_logger import get_structured_logger
from refinedfinishedtrips.services.extract_trips_per_line_per_vehicle import (
    extract_trips_per_line_per_vehicle,
)

structured_logger = get_structured_logger(logger_name=__name__)

def _get_count_of_non_circular_trips_with_distance(trips: list) -> int:
    _TRIP_IS_CIRCULAR = 5
    return sum(1 for t in trips if not t[_TRIP_IS_CIRCULAR])


def _build_extraction_metrics(
    total_trips: int,
    total_source_sentido_discrepancies: int,
    total_input_position_sanitization_drops: int,
    total_input_position_records: int,
    vehicle_line_groups_processed: int,
    vehicle_line_groups_failed: int,
    non_circular_trips_with_distance: int,
) -> Dict[str, int]:
    return {
        "total_finished_trips": total_trips,
        "total_source_sentido_discrepancies": total_source_sentido_discrepancies,
        "total_input_position_sanitization_drops": total_input_position_sanitization_drops,
        "total_input_position_records": total_input_position_records,
        "vehicle_line_groups_processed": vehicle_line_groups_processed,
        "vehicle_line_groups_failed": vehicle_line_groups_failed,
        "non_circular_trips_with_distance": non_circular_trips_with_distance,
    }


def get_all_finished_trips(
    config: Dict[str, Any],
    df_recent_positions: pd.DataFrame,
    _extract_trips_fn=extract_trips_per_line_per_vehicle,
) -> Tuple[List[Tuple], Dict[str, int]]:
    def get_config(config):
        return config["general"]["trip_detection"]["stop_proximity_threshold_meters"]

    stop_proximity_threshold_meters = get_config(config)
    positions_list = cast(List[Dict[str, Any]], df_recent_positions.to_dict("records"))
    total_input_position_records = len(positions_list)
    num_processed = 0
    num_failed = 0
    total_source_sentido_discrepancies = 0
    total_input_position_sanitization_drops = 0
    all_finished_trips = []
    current_vehicle_key: Optional[Tuple[str, str]] = None
    start_idx = 0
    for i, position in enumerate(positions_list):
        linha_lt = position["linha_lt"]
        veiculo_id = str(position["veiculo_id"])
        vehicle_key = (linha_lt, veiculo_id)
        is_last = i == len(positions_list) - 1
        previous_vehicle_key = current_vehicle_key
        vehicle_changed = previous_vehicle_key != vehicle_key
        if vehicle_changed and previous_vehicle_key is not None:
            prev_linha_lt, prev_veiculo_id = previous_vehicle_key
            try:
                finished_trips, source_sentido_discrepancies, dropped_points = _extract_trips_fn(
                    positions_list, start_idx, i - 1, prev_linha_lt, prev_veiculo_id, stop_proximity_threshold_meters
                )
                if finished_trips:
                    all_finished_trips.extend(finished_trips)
                total_source_sentido_discrepancies += source_sentido_discrepancies
                total_input_position_sanitization_drops += dropped_points
                num_processed += 1
            except Exception:
                num_failed += 1
            if (num_processed + num_failed) % 500 == 0:
                structured_logger.info(
                    event="trip_extraction_progress",
                    message="Trip extraction in progress",
                    metadata={"vehicle_line_groups_processed": num_processed},
                )
            start_idx = i
        current_vehicle_key = vehicle_key
        if is_last:
            try:
                finished_trips, source_sentido_discrepancies, dropped_points = _extract_trips_fn(
                    positions_list, start_idx, i, linha_lt, veiculo_id, stop_proximity_threshold_meters
                )
                if finished_trips:
                    all_finished_trips.extend(finished_trips)
                total_source_sentido_discrepancies += source_sentido_discrepancies
                total_input_position_sanitization_drops += dropped_points
                num_processed += 1
            except Exception:
                num_failed += 1
    total_trips = len(all_finished_trips)
    non_circular_trips_with_distance = _get_count_of_non_circular_trips_with_distance(all_finished_trips)
    extraction_metrics = _build_extraction_metrics(
        total_trips=total_trips,
        total_source_sentido_discrepancies=total_source_sentido_discrepancies,
        total_input_position_sanitization_drops=total_input_position_sanitization_drops,
        total_input_position_records=total_input_position_records,
        vehicle_line_groups_processed=num_processed,
        vehicle_line_groups_failed=num_failed,
        non_circular_trips_with_distance=non_circular_trips_with_distance,
    )
    structured_logger.info(
        event="trip_extraction_completed",
        message="Trip extraction completed",
        metadata=extraction_metrics,
    )
    return all_finished_trips, extraction_metrics
