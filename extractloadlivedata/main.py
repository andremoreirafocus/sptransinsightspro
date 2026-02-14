from src.services.extract_buses_positions import (
    extract_buses_positions_with_retries,
    get_buses_positions_with_metadata,
)
from src.services.save_bus_positions import (
    save_bus_positions_to_storage,
    save_bus_positions_to_local_volume,
)

from src.infra.timing_functions import adjust_start_time, interval_adjustment_needed
from src.config import get_config
import time
import logging
from logging.handlers import RotatingFileHandler

LOG_FILENAME = "extractlivedata.log"

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


def main():
    logger = logging.getLogger(__name__)
    config = get_config()
    INTERVAL = int(config["EXTRACTION_INTERVAL_SECONDS"])
    # adjust_start_time()
    while True:
        delta = interval_adjustment_needed()
        previous_epoch_time = time.time()
        buses_positions_payload = extract_buses_positions_with_retries(config)
        download_successful = buses_positions_payload is not None
        if download_successful:
            buses_positions, _ = get_buses_positions_with_metadata(
                buses_positions_payload
            )
            save_bus_positions_to_local_volume(config, buses_positions)
            save_bus_positions_to_storage(config, buses_positions)
        current_epoch_time = time.time()
        duration = current_epoch_time - previous_epoch_time
        interval = INTERVAL - duration + delta
        if delta != 0:
            logger.info(f"Calculated interval adjusted by {delta} seconds.")
        logger.info(
            f"[*] Waiting for {interval:.0f} seconds until next extraction due to duration {duration:.0f} second(s)...\n"
        )
        time.sleep(interval)


if __name__ == "__main__":
    main()
