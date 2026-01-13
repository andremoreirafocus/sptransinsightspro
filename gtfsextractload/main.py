from src.services.extract_gtfs_files import extract_gtfs_files
from src.services.load_files_to_raw import load_files_to_raw
from src.config import get_config
import logging
from logging.handlers import RotatingFileHandler

LOG_FILENAME = "gtfsextractload.log"

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
    config = get_config()
    files_list = extract_gtfs_files(config)
    load_files_to_raw(config, files_list)


if __name__ == "__main__":
    main()
