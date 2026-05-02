import logging
from typing import Any, Dict, List, Tuple

import pandas as pd

from refinedfinishedtrips.services.extract_trips_per_line_per_vehicle import (
    extract_trips_per_line_per_vehicle,
)

logger = logging.getLogger(__name__)


def get_all_finished_trips(config: Dict[str, Any], df_recent_positions: pd.DataFrame) -> List[Tuple]:
    def get_config(config):
        return config["general"]["trip_detection"]["stop_proximity_threshold_meters"]

    stop_proximity_threshold_meters = get_config(config)
    positions_list = df_recent_positions.to_dict("records")
    total_input_position_records = len(positions_list)
    num_processed = 0
    total_sentido_mismatches = 0
    total_sanitization_drops = 0
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
            finished_trips, mismatches, dropped_points = extract_trips_per_line_per_vehicle(
                positions_list, start_idx, i - 1, prev_linha_lt, prev_veiculo_id, stop_proximity_threshold_meters
            )
            if finished_trips:
                all_finished_trips.extend(finished_trips)
            total_sentido_mismatches += mismatches
            total_sanitization_drops += dropped_points
            num_processed += 1
            if num_processed % 500 == 0:
                logger.info(
                    f"Progress: {num_processed} vehicle/line combinations processed."
                )
            start_idx = i
        current_vehicle = vehicle_key
        if is_last:
            finished_trips, mismatches, dropped_points = extract_trips_per_line_per_vehicle(
                positions_list, start_idx, i, linha_lt, veiculo_id, stop_proximity_threshold_meters
            )
            if finished_trips:
                all_finished_trips.extend(finished_trips)
            total_sentido_mismatches += mismatches
            total_sanitization_drops += dropped_points
            num_processed += 1
    total_trips = len(all_finished_trips)
    logger.info(f"Progress: {num_processed} vehicle/line combinations processed.")
    logger.info(f"Total finished trips: {total_trips}, sentido mismatches: {total_sentido_mismatches}/{total_trips}")
    logger.info(
        f"Total invalid position records dropped by sanitization: {total_sanitization_drops} "
        f"out of {total_input_position_records}"
    )
    return all_finished_trips
