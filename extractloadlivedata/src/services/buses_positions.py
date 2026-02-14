import requests
import time
from datetime import datetime
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def extract_buses_positions_with_retries(config):
    def get_config(config):
        token = config["TOKEN"]
        api_base_url = config["API_BASE_URL"]
        api_max_retries = int(config["API_MAX_RETRIES"])

        return api_max_retries, token, api_base_url

    api_max_retries, token, api_base_url = get_config(config)

    retries = 0
    back_off = 1
    buses_positions_payload = None
    download_successful = False
    while not download_successful:
        buses_positions_payload = extract_buses_positions(
            token=token,
            base_url=api_base_url,
        )
        if buses_positions_response_is_valid(buses_positions_payload):
            download_successful = True
            if retries > 0:
                logger.info(
                    f"Download successful after {retries} {'retry' if retries == 1 else 'retries'}."
                )
            return buses_positions_payload
        retries += 1
        logger.warning(
            f"Invalid buses positions response structure! Retrying in {back_off} seconds..."
        )
        time.sleep(back_off)
        back_off *= 2
        if retries >= api_max_retries:
            download_successful = False
            logger.error(
                "Max retries reached. Download failed. Skipping this extraction cycle."
            )
    return None


def get_buses_positions_with_metadata(buses_positions_payload):
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
    return buses_positions, reference_time


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
        logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Get posicao started!")
        response = session.get(posicao_url)
        if response.status_code == 200:
            logger.info(
                f"[{datetime.now().strftime('%H:%M:%S')}] Get posicao status OK!"
            )
            data = response.json()
            return data
        else:
            logger.error(f"Error getting positions: {response.status_code}")
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        return None


def buses_positions_response_is_valid(buses_positions):
    """
    Validate the structure of the incoming buses_positions.
    :param buses_positions: The buses_positions to validate
    :return: True if valid, False otherwise
    """
    if not isinstance(buses_positions, dict):
        logger.error("Payload does not have a valid structure.")
        logger.error(f"Payload content: {buses_positions}")
        return False
    required_fields = ["hr", "l"]
    for field in required_fields:
        if field not in buses_positions:
            logger.error(f"Missing required payload field: {field}")
            return False
    return True


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
