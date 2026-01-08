from datetime import datetime
from dotenv import dotenv_values
from src.services.buses_positions import (
    extract_buses_positions,
    get_buses_positions_summary,
)
from src.infra.storage import save_data_to_json_file
from src.infra.message_broker import sendKafka
import json
import time


def main():
    config = dotenv_values(".env")
    while True:
        buses_positions_payload = extract_buses_positions(
            token=config.get("TOKEN"),
            base_url=config.get("API_BASE_URL"),
        )
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
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Ref SPTrans: {reference_time} | Veículos Ativos: {total_vehicles}"
        )
        save_data_to_json_file(
            buses_positions,
            downloads_folder=config.get("DOWNLOADS_FOLDER"),
            file_name=f"buses_positions_{reference_time}.json",
        )
        sendKafka(
            topic=config.get("KAFKA_TOPIC"),
            # message=buses_positions,
            message=json.dumps(buses_positions),
            broker=config.get("KAFKA_BROKER"),
        )
        interval = int(config.get("EXTRACTION_INTERVAL_SECONDS"))
        print(f"[*] Waiting for {interval} seconds until next extraction...\n")
        time.sleep(interval)


if __name__ == "__main__":
    main()
