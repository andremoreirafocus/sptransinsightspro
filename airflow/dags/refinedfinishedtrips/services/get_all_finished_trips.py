import logging
from typing import List, Tuple

import pandas as pd

from refinedfinishedtrips.services.extract_trips_per_line_per_vehicle import (
    extract_trips_per_line_per_vehicle,
)

logger = logging.getLogger(__name__)


def get_all_finished_trips(df_recent_positions: pd.DataFrame) -> List[Tuple]:
    positions_list = df_recent_positions.to_dict("records")
    num_processed = 0
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
            finished_trips = extract_trips_per_line_per_vehicle(
                positions_list, start_idx, i - 1, prev_linha_lt, prev_veiculo_id
            )
            if finished_trips:
                all_finished_trips.extend(finished_trips)
            num_processed += 1
            if num_processed % 500 == 0:
                logger.info(
                    f"Progress: {num_processed} vehicle/line combinations processed."
                )
            start_idx = i
        current_vehicle = vehicle_key
        if is_last:
            finished_trips = extract_trips_per_line_per_vehicle(
                positions_list, start_idx, i, linha_lt, veiculo_id
            )
            if finished_trips:
                all_finished_trips.extend(finished_trips)
            num_processed += 1
    logger.info(f"Progress: {num_processed} vehicle/line combinations processed.")
    logger.info(f"Total finished trips: {len(all_finished_trips)}")
    return all_finished_trips
