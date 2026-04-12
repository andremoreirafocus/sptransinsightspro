from updatelatestpositions.services.create_latest_positions import (
    create_latest_positions_table,
)
from pipeline_configurator.config import get_config
from updatelatestpositions.config.updatelatestpositions_config_schema import (
    GeneralConfig,
)

import logging
from logging.handlers import RotatingFileHandler

LOG_FILENAME = "updatelatestpositions.log"

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
PIPELINE_NAME = "updatelatestpositions"


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


def update_latest_positions_table():
    pipeline_config = _load_pipeline_config()
    create_latest_positions_table(pipeline_config)


def main():
    update_latest_positions_table()


if __name__ == "__main__":
    main()
