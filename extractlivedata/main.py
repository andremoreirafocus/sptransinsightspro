from src.services.buses_positions import (
    extract_buses_positions,
    buses_positions_response_is_valid,
    get_buses_positions_summary,
)
from src.infra.storage import save_data_to_json_file
from src.infra.message_broker import sendKafka
from src.infra.check_disk_space import is_disk_space_ok
from src.infra.adjust_interval import adjust_time
from src.config import get_config
from datetime import datetime
import json
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
    TOKEN = config["TOKEN"]
    API_BASE_URL = config["API_BASE_URL"]
    KAFKA_TOPIC = config["KAFKA_TOPIC"]
    KAFKA_BROKER = config["KAFKA_BROKER"]
    DOWNLOADS_FOLDER = config["DOWNLOADS_FOLDER"]
    API_MAX_RETRIES = int(config["API_MAX_RETRIES"])
    INTERVAL = int(config["EXTRACTION_INTERVAL_SECONDS"])
    adjust_time()
    while True:
        if not is_disk_space_ok():
            logger.error("Disk space critical: skipping execution")
            print("Disk space critical: skipping execution")
            time.sleep(INTERVAL)
            continue
        retries = 0
        back_off = 1
        buses_positions_payload = None
        download_successful = False
        previous_epoch_time = time.time()
        while not download_successful:
            buses_positions_payload = extract_buses_positions(
                token=TOKEN,
                base_url=API_BASE_URL,
            )
            if buses_positions_response_is_valid(buses_positions_payload):
                download_successful = True
                if retries > 0:
                    logger.info(
                        f"Download successful after {retries} {'retry' if retries == 1 else 'retries'}."
                    )
                break
            retries += 1
            logger.warning(
                f"Invalid buses positions response structure! Retrying in {back_off} seconds..."
            )
            time.sleep(back_off)
            back_off *= 2
            if retries >= API_MAX_RETRIES:
                download_successful = False
                logger.error(
                    "Max retries reached. Download failed. Skipping this extraction cycle."
                )
                break
        if download_successful:
            reference_time, total_vehicles = get_buses_positions_summary(
                buses_positions_payload
            )
            buses_positions = {
                "metadata": {
                    "extracted_at": datetime.now().isoformat(),
                    "source": "sptrans_api_v2",
                    "total_vehicles": total_vehicles,
                },
                "payload": buses_positions_payload,
            }
            logger.info(
                f"[{datetime.now().strftime('%H:%M:%S')}] Ref SPTrans: {reference_time} | Veículos Ativos: {total_vehicles}"
            )
            save_data_to_json_file(
                buses_positions,
                downloads_folder=DOWNLOADS_FOLDER,
                file_name=f"buses_positions_{reference_time}.json",
            )
            sendKafka(
                broker=KAFKA_BROKER,
                topic=KAFKA_TOPIC,
                message=json.dumps(buses_positions),
            )
        current_epoch_time = time.time()
        duration = current_epoch_time - previous_epoch_time
        interval = INTERVAL - duration
        time.sleep(interval)
        logger.info(
            f"[*] Waiting for {interval:.0f} seconds until next extraction due to duration {duration:.0f} second(s)...\n"
        )


if __name__ == "__main__":
    main()
