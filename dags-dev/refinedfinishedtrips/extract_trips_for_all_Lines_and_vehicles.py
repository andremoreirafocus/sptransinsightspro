from refinedfinishedtrips.services.get_all_finished_trips import (
    get_all_finished_trips,
)

from refinedfinishedtrips.services.get_recent_positions import get_recent_positions
from refinedfinishedtrips.services.save_finished_trips_to_db import (
    save_finished_trips_to_db,
)
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def extract_trips_for_all_Lines_and_vehicles( 
    config,
    get_recent_positions_fn=get_recent_positions,
    save_trips_fn=save_finished_trips_to_db,
    extract_trips_fn=get_all_finished_trips,
):
    df_recent_positions = get_recent_positions_fn(config)
    if df_recent_positions.empty:
        logger.warning("No position data found for the period.")
        return
    all_finished_trips = extract_trips_fn(df_recent_positions)
    save_trips_fn(config, all_finished_trips)
