import requests
from datetime import datetime
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def extract_buses_positions(base_url, token):
    session = requests.Session()

    auth_url = f"{base_url}/Login/Autenticar?token={token}"
    try:
        response_auth = session.post(auth_url)
        if response_auth.status_code == 200 and response_auth.text.lower() == "true":
            logger.info(
                f"[{datetime.now().strftime('%H:%M:%S')}] Succesfully authenticated!"
            )
        else:
            logger.error("Authentication error. Verify your Token.")
            logger.error(response_auth.status_code, response_auth.text)
            return
    except Exception as e:
        logger.error(f"Error connecting: {e}")
        return None

    try:
        posicao_url = f"{base_url}/Posicao"
        logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Download started!")
        response = session.get(posicao_url)
        logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Download finished!")

        if response.status_code == 200:
            data = response.json()
            return data

        else:
            logger.error(f"Error getting positions: {response.status_code}")

    except Exception as e:
        logger.error(f"Error during execution: {e}")
        return None


def get_buses_positions_summary(buses_positions):
    if not isinstance(buses_positions, dict):
        logger.error(f"Incorrect data type: {type(buses_positions)}")
        return "NaN", "NaN"
    try:
        reference_time = buses_positions.get("hr", "NaN")
        lines = buses_positions.get("l", [])  # 'l' contém a lista de lines e veículos
        total_vehicles = sum([len(line.get("vs", [])) for line in lines])
        return reference_time, total_vehicles
    except Exception as e:
        logger.error(f"Error processing positions summary: {e}")
        return "NaN", "NaN"
