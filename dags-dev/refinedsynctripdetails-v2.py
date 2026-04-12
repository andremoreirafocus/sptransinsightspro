from refinedsynctripdetails.services.load_trip_details_from_storage_to_dataframe import (
    load_trip_details_from_storage_to_dataframe,
)
from refinedsynctripdetails.services.save_trip_details_from_dataframe_to_refined import (
    save_trip_details_from_dataframe_to_refined,
)
from pipeline_configurator.config import get_config
from refinedsynctripdetails.config.refinedsynctripdetails_config_schema import (
    GeneralConfig,
)
import logging
from logging.handlers import RotatingFileHandler

LOG_FILENAME = "refinedsynctripdetails.log"

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
PIPELINE_NAME = "refinedsynctripdetails"


def _load_pipeline_config():
    try:
        pipeline_config = get_config(
            PIPELINE_NAME,
            None,
            GeneralConfig,
            None,
            "minio_conn",
            "postgres_conn",
            load_raw_data_json_schema=False,
            load_data_expectations=False,
        )
    except Exception as e:
        logger.error(f"Pipeline configuration validation failed: {e}")
        raise ValueError(f"Pipeline configuration validation failed: {e}")
    return pipeline_config


def refined_sync_trip_details(config):

    df_trip_details = load_trip_details_from_storage_to_dataframe(config)
    save_trip_details_from_dataframe_to_refined(config, df_trip_details)


def main():
    pipeline_config = _load_pipeline_config()
    refined_sync_trip_details(pipeline_config)


if __name__ == "__main__":
    main()
