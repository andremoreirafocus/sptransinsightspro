from gtfs.extractload.services.extract_gtfs_files import extract_gtfs_files
from gtfs.extractload.services.load_files_to_raw import load_files_to_raw
from gtfs.extractload.config import get_config as get_config_extractload

from gtfs.transform.services.create_save_trip_details import (
    create_trip_details_table,
    create_trip_details_table_and_fill_missing_data,
)
from gtfs.transform.services.transforms import (
    transform_calendar,
    transform_frequencies,
    transform_routes,
    transform_stop_times,
    transform_stops,
    transform_trips,
)
from gtfs.transform.config import get_config as get_config_transform
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


def extract_load_files():
    config = get_config_extractload()
    files_list = extract_gtfs_files(config)
    load_files_to_raw(config, files_list)


def transform():
    logging.info("Starting GTFS Transformations...")
    config = get_config_transform()
    transform_routes(config)
    transform_trips(config)
    transform_stops(config)
    transform_stop_times(config)
    transform_frequencies(config)
    transform_calendar(config)
    create_trip_details_table(config)
    create_trip_details_table_and_fill_missing_data(config)
    logger.info("All transformations completed successfully.")


def main():
    #
    extract_load_files()
    transform()


if __name__ == "__main__":
    main()
