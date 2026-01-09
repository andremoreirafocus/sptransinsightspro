from dotenv import dotenv_values
from src.services.transforms import (
    transform_position,
)
import logging
from logging.handlers import RotatingFileHandler

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
    config = dotenv_values(".env")
    source_bucket = config["SOURCE_BUCKET"]
    app_folder = config["APP_FOLDER"]
    db_connection = None
    table_name = config["TABLE_NAME"]
    transform_position(source_bucket, app_folder, db_connection, table_name)

    if __name__ == "__main__":
        main()
