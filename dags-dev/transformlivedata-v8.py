from transformlivedata.transformlivedata import load_transform_save_positions
import logging
from logging.handlers import RotatingFileHandler

PIPELINE_NAME = "transformlivedata"
LOG_FILENAME = f"{PIPELINE_NAME}.log"

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
    logical_date_string = (
        "2026-04-27T15:14:00+00:00"  # Replace with the actual logical_date_string
    )
    result = load_transform_save_positions(PIPELINE_NAME, logical_date_string)
    logger.info(f"Pipeline result: {result}")


if __name__ == "__main__":
    main()
