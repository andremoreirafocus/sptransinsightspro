from refinedfinishedtrips.services.extract_trips_per_line_per_vehicle_pandas import (
    extract_trips_per_line_per_vehicle_pandas,
)

from refinedfinishedtrips.services.get_recent_positions import get_recent_positions

# from refinedfinishedtrips.services.save_trips_to_db import save_trips_to_db
from refinedfinishedtrips.services.save_finished_trips_to_db import (
    save_finished_trips_to_db,
)
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def extract_trips_for_all_Lines_and_vehicles_pandas(config):
    df_recent_positions = get_recent_positions(config)
    if df_recent_positions.empty:
        logger.warning("No position data found for the period.")
        return
    
    # Single conversion at top level (already sorted by SQL)
    positions_list = df_recent_positions.to_dict("records")
    
    # Single pass with index-based vehicle change detection
    num_processed = 0
    all_finished_trips = []
    current_vehicle = None
    start_idx = 0
    
    for i, position in enumerate(positions_list):
        linha_lt = position["linha_lt"]
        veiculo_id = position["veiculo_id"]
        vehicle_key = (linha_lt, veiculo_id)
        
        # Check if vehicle changed or if we're at the last position
        is_last = (i == len(positions_list) - 1)
        vehicle_changed = (current_vehicle is not None and current_vehicle != vehicle_key)
        
        if vehicle_changed:
            # Process the previous vehicle
            prev_linha_lt, prev_veiculo_id = current_vehicle
            finished_trips = extract_trips_per_line_per_vehicle_pandas(
                positions_list, start_idx, i - 1, prev_linha_lt, prev_veiculo_id
            )
            if finished_trips:
                all_finished_trips.extend(finished_trips)
            num_processed += 1
            if num_processed % 500 == 0:
                logger.info(f"Progress: {num_processed} vehicle/line combinations processed.")
            # Start new vehicle
            start_idx = i
        
        current_vehicle = vehicle_key
        
        # Process last vehicle
        if is_last:
            finished_trips = extract_trips_per_line_per_vehicle_pandas(
                positions_list, start_idx, i, linha_lt, veiculo_id
            )
            if finished_trips:
                all_finished_trips.extend(finished_trips)
            num_processed += 1
    
    logger.info(f"Progress: {num_processed} vehicle/line combinations processed.")
    logger.info(f"Total finished trips: {len(all_finished_trips)}")
    # save_trips_to_db(config, all_finished_trips)
    save_finished_trips_to_db(config, all_finished_trips)
