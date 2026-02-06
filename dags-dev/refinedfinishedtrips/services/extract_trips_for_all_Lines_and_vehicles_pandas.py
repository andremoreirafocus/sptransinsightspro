from refinedfinishedtrips.services.extract_trips_per_line_per_vehicle_pandas import (
    extract_trips_per_line_per_vehicle_pandas,
)

from refinedfinishedtrips.services.get_recent_positions import get_recent_positions
from refinedfinishedtrips.services.save_trips_to_db import save_trips_to_db
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def extract_trips_for_all_Lines_and_vehicles_pandas(config):
    df_recent_positions = get_recent_positions(config)
    if df_recent_positions.empty:
        logger.warning("No position data found for the period.")
        return
    grouped = df_recent_positions.groupby(["linha_lt", "veiculo_id"])
    total_groups = len(grouped)
    logger.info(
        f"Processing {total_groups} unique line/vehicle combinations in memory."
    )
    num_processed = 0
    all_finished_trips = []
    for (linha_lt, veiculo_id), df_group in grouped:
        finished_trips = extract_trips_per_line_per_vehicle_pandas(
            linha_lt, veiculo_id, df_group
        )
        if finished_trips:
            for finished_trip in finished_trips:
                all_finished_trips.append(finished_trip)
        num_processed += 1
        if num_processed % 500 == 0:
            logger.info(f"Progress: {num_processed}/{total_groups} processed.")
    logger.info(f"Progress: {num_processed}/{total_groups} processed.")
    logger.info(f"Total finished trips: {len(all_finished_trips)}")
    save_trips_to_db(config, all_finished_trips)
