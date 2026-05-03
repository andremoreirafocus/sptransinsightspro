import logging
from typing import Any, Dict, List, Tuple

import pandas as pd

from refinedfinishedtrips.services.extract_trips_per_line_per_vehicle import (
    extract_trips_per_line_per_vehicle,
)

logger = logging.getLogger(__name__)


def _build_extraction_metrics(
    total_trips: int,
    total_source_sentido_discrepancies: int,
    total_input_position_sanitization_drops: int,
    total_input_position_records: int,
    vehicle_line_groups_processed: int,
) -> Dict[str, int]:
    return {
        "total_finished_trips": total_trips,
        "total_source_sentido_discrepancies": total_source_sentido_discrepancies,
        "total_input_position_sanitization_drops": total_input_position_sanitization_drops,
        "total_input_position_records": total_input_position_records,
        "vehicle_line_groups_processed": vehicle_line_groups_processed,
    }


def get_all_finished_trips(
    config: Dict[str, Any], df_recent_positions: pd.DataFrame
) -> Tuple[List[Tuple], Dict[str, int]]:
    def get_config(config):
        return config["general"]["trip_detection"]["stop_proximity_threshold_meters"]

    stop_proximity_threshold_meters = get_config(config)
    positions_list = df_recent_positions.to_dict("records")
    total_input_position_records = len(positions_list)
    num_processed = 0
    total_source_sentido_discrepancies = 0
    total_input_position_sanitization_drops = 0
    all_finished_trips = []
    current_vehicle = None
    start_idx = 0
    for i, position in enumerate(positions_list):
        linha_lt = position["linha_lt"]
        veiculo_id = position["veiculo_id"]
        vehicle_key = (linha_lt, veiculo_id)
        is_last = i == len(positions_list) - 1
        vehicle_changed = current_vehicle is not None and current_vehicle != vehicle_key
        if vehicle_changed:
            prev_linha_lt, prev_veiculo_id = current_vehicle
            finished_trips, source_sentido_discrepancies, dropped_points = extract_trips_per_line_per_vehicle(
                positions_list, start_idx, i - 1, prev_linha_lt, prev_veiculo_id, stop_proximity_threshold_meters
            )
            if finished_trips:
                all_finished_trips.extend(finished_trips)
            total_source_sentido_discrepancies += source_sentido_discrepancies
            total_input_position_sanitization_drops += dropped_points
            num_processed += 1
            if num_processed % 500 == 0:
                logger.info(
                    f"Progress: {num_processed} vehicle/line combinations processed."
                )
            start_idx = i
        current_vehicle = vehicle_key
        if is_last:
            finished_trips, source_sentido_discrepancies, dropped_points = extract_trips_per_line_per_vehicle(
                positions_list, start_idx, i, linha_lt, veiculo_id, stop_proximity_threshold_meters
            )
            if finished_trips:
                all_finished_trips.extend(finished_trips)
            total_source_sentido_discrepancies += source_sentido_discrepancies
            total_input_position_sanitization_drops += dropped_points
            num_processed += 1
    total_trips = len(all_finished_trips)
    extraction_metrics = _build_extraction_metrics(
        total_trips=total_trips,
        total_source_sentido_discrepancies=total_source_sentido_discrepancies,
        total_input_position_sanitization_drops=total_input_position_sanitization_drops,
        total_input_position_records=total_input_position_records,
        vehicle_line_groups_processed=num_processed,
    )
    logger.info(f"Progress: {num_processed} vehicle/line combinations processed.")
    logger.info(
        "Total finished trips: %s, source sentido discrepancies: %s/%s",
        total_trips,
        total_source_sentido_discrepancies,
        total_trips,
    )
    logger.info(
        "Total invalid position records dropped by sanitization: %s out of %s",
        total_input_position_sanitization_drops,
        total_input_position_records,
    )
    logger.info(
        "Trip extraction metrics: %s",
        extraction_metrics,
    )
    return all_finished_trips, extraction_metrics
