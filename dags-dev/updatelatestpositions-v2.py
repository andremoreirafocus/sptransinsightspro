from updatelatestpositions.services.create_latest_positions import (
    create_latest_positions_table,
)
from updatelatestpositions.config import get_config

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


def update_latest_positions_table():
    config = get_config()
    create_latest_positions_table(config)


def main():
    update_latest_positions_table()


if __name__ == "__main__":
    main()
