from transformlivedata.services.load_positions import load_positions
from transformlivedata.services.transform_positions import transform_positions
from transformlivedata.services.save_positions_to_storage import (
    save_positions_to_storage,
)
from transformlivedata.services.processed_requests_helper import (
    mark_request_as_processed,
)
from transformlivedata.config import get_config
from datetime import datetime
from zoneinfo import ZoneInfo
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


def load_transform_save_positions(logical_date_string):
    config = get_config()
    dt_utc = datetime.fromisoformat(logical_date_string)
    dt = dt_utc.astimezone(ZoneInfo("America/Sao_Paulo"))
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    hour = dt.strftime("%H")
    minute = dt.strftime("%M")
    logging.info(f"Transforming position for {dt}...")
    # year = "2026"
    # month = "02"
    # day = "15"
    # hour = "09"
    # minute = "54"
    raw_positions = load_positions(config, year, month, day, hour, minute)
    if not raw_positions:
        logging.error("No position data found to transform.")
        raise ValueError("No position data found to transform.")
    positions_table = transform_positions(config, raw_positions)
    if not positions_table:
        logging.error("No valid position records found after transformation.")
        raise ValueError("No valid position records found after transformation.")
    save_positions_to_storage(config, positions_table)
    mark_request_as_processed(config, logical_date_string)


def main():
    logical_date_string = (
        "2026-02-26T20:36:00+00:00"  # Replace with the actual logical_date_string
    )
    load_transform_save_positions(logical_date_string)


if __name__ == "__main__":
    main()
