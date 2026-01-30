from src.tasks.trips.pandas.extract_trips_for_all_Lines_and_vehicles_pandas import (
    extract_trips_for_all_Lines_and_vehicles_pandas,
)

# from src.update_latest_positions import update_latest_positions
import logging
from logging.handlers import RotatingFileHandler
from src.config import get_config

LOG_FILENAME = "refinelivedata.log"

# In Airflow just remove this logging configuration block
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        # Rotation: 5MB per file, keeping the last 5 files
        RotatingFileHandler(LOG_FILENAME, maxBytes=5 * 1024 * 1024, backupCount=5),
        logging.StreamHandler(),  # Also keeps console output
    ],
)

logger = logging.getLogger(__name__)


def main():
    logger.info("Starting Refinement process...")
    print("Starting Refinement process...")
    config = get_config()
    extract_trips_for_all_Lines_and_vehicles_pandas(config)
    # update_latest_positions(config)

    # generate_trips_info_incrementally(config)


if __name__ == "__main__":
    main()
