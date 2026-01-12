from datetime import datetime
from dotenv import dotenv_values
from src.services.buses_positions import (
    extract_buses_positions,
    buses_positions_response_is_valid,
    get_buses_positions_summary,
)
from src.infra.storage import save_data_to_json_file
from src.infra.message_broker import sendKafka
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

logger = logging.getLogger(__name__)


def main():
    config = dotenv_values(".env")
    TOKEN = config.get("TOKEN")
    API_BASE_URL = config.get("API_BASE_URL")
    KAFKA_TOPIC = config.get("KAFKA_TOPIC")
    KAFKA_BROKER = config.get("KAFKA_BROKER")
    DOWNLOADS_FOLDER = config.get("DOWNLOADS_FOLDER")
    while True:
        buses_positions_payload = extract_buses_positions(
            token=TOKEN,
            base_url=API_BASE_URL,
        )
        if not buses_positions_response_is_valid(buses_positions_payload):
            logger.error("Invalid buses positions response structure. Skipping...")
            continue
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
        interval = int(config.get("EXTRACTION_INTERVAL_SECONDS"))
        logger.info(f"[*] Waiting for {interval} seconds until next extraction...\n")
        time.sleep(interval)


if __name__ == "__main__":
    main()
