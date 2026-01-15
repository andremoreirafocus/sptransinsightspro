from src.services.create_save_trip_details import (
    create_trip_details_table,
)
from src.services.transforms import (
    transform_calendar,
    transform_frequencies,
    transform_routes,
    transform_stop_times,
    transform_stops,
    transform_trips,
)
from src.config import get_config
import logging
from logging.handlers import RotatingFileHandler

LOG_FILENAME = "gtfstransform.log"

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
    logger.info("Starting GTFS Transformations...")
    config = get_config()
    transform_routes(config)
    transform_trips(config)
    transform_stops(config)
    transform_stop_times(config)
    transform_frequencies(config)
    transform_calendar(config)
    create_trip_details_table(config)
    logger.info("All transformations completed successfully.")


if __name__ == "__main__":
    main()
