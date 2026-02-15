from transformlivedata.services.load_positions import load_positions
from transformlivedata.services.transform_positions import transform_positions
from transformlivedata.services.save_positions_to_storage import (
    save_positions_to_storage,
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


def load_positions_from_raw():
    # def load_positions_from_raw(logical_date_string, **kwargs):
    config = get_config()
    # dt_utc = datetime.fromisoformat(logical_date_string)
    # dt = dt_utc.astimezone(ZoneInfo("America/Sao_Paulo"))
    # year = dt.strftime("%Y")
    # month = dt.strftime("%m")
    # day = dt.strftime("%d")
    # hour = dt.strftime("%H")
    # minute = dt.strftime("%M")
    # logging.info(f"Transforming position for {dt}...")
    # year = "2026"
    # month = "02"
    # day = "04"
    # hour = "11"
    # minute = "24"
    year = "2026"
    month = "02"
    day = "15"
    hour = "09"
    minute = "54"

    raw_positions = load_positions(config, year, month, day, hour, minute)
    if not raw_positions:
        logging.error("No position data found to transform.")
        raise ValueError("No position data found to transform.")
    return raw_positions


def transform_positions_to_in_memory_table(raw_positions):
    # def transform_positions_to_in_memory_table(ti):
    # raw_positions = ti.xcom_pull(task_ids="load_positions_from_raw")
    config = get_config()
    positions_table = transform_positions(config, raw_positions)
    if not positions_table:
        logging.error("No valid position records found after transformation.")
        raise ValueError("No valid position records found after transformation.")
    return positions_table


def save_to_storage(positions_table):
    # def save_to_db(ti):
    #    positions_table = ti.xcom_pull(task_ids="transform_positions_to_in_memory_table")
    config = get_config()
    save_positions_to_storage(config, positions_table)


def main():
    raw_positions = load_positions_from_raw()
    positions_table = transform_positions_to_in_memory_table(raw_positions)
    save_to_storage(positions_table)


if __name__ == "__main__":
    main()
