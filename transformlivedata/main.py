from src.services.load_and_transform_positions import (
    load_and_transform_positions,
)
import logging
from logging.handlers import RotatingFileHandler
from src.config import get_config

LOG_FILENAME = "transformlivedata.log"

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
    logger.info("Starting transformation process...")
    print("Starting transformation process...")
    config = get_config()
    year = "2026"
    month = "01"
    day = "12"
    hour = "16"
    minute = "00"
    # year = "2026"
    # month = "01"
    # day = "10"
    # hour = "08"
    # minute = "42"

    load_and_transform_positions(config, year, month, day, hour, minute)


if __name__ == "__main__":
    main()
