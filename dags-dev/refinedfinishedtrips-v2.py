from refinedfinishedtrips.services.extract_trips_for_all_Lines_and_vehicles_pandas import (
    extract_trips_for_all_Lines_and_vehicles_pandas as extract_trips_for_all_Lines_and_vehicles,
)
from refinedfinishedtrips.config import get_config

import logging
from logging.handlers import RotatingFileHandler

LOG_FILENAME = "refinedfinishedtrips.log"

# In Airflow just remove this logging configuration block
logging.basicConfig(
    # level=logging.DEBUG,
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        # Rotation: 5MB per file, keeping the last 5 files
        RotatingFileHandler(LOG_FILENAME, maxBytes=5 * 1024 * 1024, backupCount=5),
        logging.StreamHandler(),  # Also keeps console output
    ],
)

logger = logging.getLogger(__name__)


def extract_trips():
    config = get_config()
    extract_trips_for_all_Lines_and_vehicles(config)


def main():
    extract_trips()


if __name__ == "__main__":
    main()
