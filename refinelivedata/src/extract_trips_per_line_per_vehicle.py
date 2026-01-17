import logging

from refinelivedata.src.services import load_positions_for_line_and_vehicle
from refinelivedata.src.services import calculate_trips
from refinelivedata.src.services import save_trips_to_db

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def extract_trips_per_line_per_vehicle(config, year, month, day, linha_lt, veiculo_id):
    filtered_positions = load_positions_for_line_and_vehicle(
        config, year, month, day, linha_lt, veiculo_id
    )
    trips = calculate_trips(filtered_positions)
    save_trips_to_db(config, trips)
