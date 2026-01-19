from src.extract_trips_per_line_per_vehicle import (
    extract_trips_per_line_per_vehicle,
)
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
    year = "2026"
    month = "01"
    day = "15"
    linha_lt = "2290-10"
    veiculo_id = "41539"

    extract_trips_per_line_per_vehicle(config, year, month, day, linha_lt, veiculo_id)


if __name__ == "__main__":
    main()
