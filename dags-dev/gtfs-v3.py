from gtfs.services.extract_gtfs_files import extract_gtfs_files
from gtfs.services.save_files_to_raw_storage import (
    save_files_to_raw_storage,
)
from gtfs.config.gtfs_config_schema import GeneralConfig
from pipeline_configurator.config import get_config

from gtfs.services.create_save_trip_details import (
    create_trip_details_table_and_fill_missing_data,
)
from gtfs.services.transforms import (
    transform_calendar,
    transform_frequencies,
    transform_routes,
    transform_stop_times,
    transform_stops,
    transform_trips,
)
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

PIPELINE_NAME = "gtfs"


def _load_pipeline_config():
    try:
        pipeline_config = get_config(
            PIPELINE_NAME,
            None,
            GeneralConfig,
            "gtfs_conn",
            "minio_conn",
            None,
        )
    except Exception as e:
        logger.error(f"Pipeline configuration validation failed: {e}")
        raise ValueError(f"Pipeline configuration validation failed: {e}")
    return pipeline_config


def extract_load_files():
    pipeline_config = _load_pipeline_config()
    files_list = extract_gtfs_files(pipeline_config)
    save_files_to_raw_storage(pipeline_config, files_list)


def transform():
    logging.info("Starting GTFS Transformations...")
    pipeline_config = _load_pipeline_config()
    transform_routes(pipeline_config)
    transform_trips(pipeline_config)
    transform_stops(pipeline_config)
    transform_stop_times(pipeline_config)
    transform_frequencies(pipeline_config)
    transform_calendar(pipeline_config)
    logger.info("All transformations completed successfully.")


def create_trip_details():
    logger.info("Creating trip details...")
    logger.info("Trip details transformation completed successfully.")
    pipeline_config = _load_pipeline_config()
    create_trip_details_table_and_fill_missing_data(pipeline_config)


def main():
    extract_load_files()
    transform()
    create_trip_details()


if __name__ == "__main__":
    main()
