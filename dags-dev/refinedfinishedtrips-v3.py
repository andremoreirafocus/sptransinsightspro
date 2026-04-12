from refinedfinishedtrips.services.extract_trips_for_all_Lines_and_vehicles_pandas import (
    extract_trips_for_all_Lines_and_vehicles_pandas as extract_trips_for_all_Lines_and_vehicles,
)
from pipeline_configurator.config import get_config
from refinedfinishedtrips.config.refinedfinishedtrips_config_schema import (
    GeneralConfig,
)

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
PIPELINE_NAME = "refinedfinishedtrips"


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


def extract_trips():
    pipeline_config = _load_pipeline_config()
    extract_trips_for_all_Lines_and_vehicles(pipeline_config)


def main():
    extract_trips()


if __name__ == "__main__":
    main()
