import requests
from datetime import datetime


def extract_buses_positions(base_url, token):
    session = requests.Session()

    auth_url = f"{base_url}/Login/Autenticar?token={token}"
    try:
        response_auth = session.post(auth_url)
        if response_auth.status_code == 200 and response_auth.text.lower() == "true":
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Succesfully authenticated!")
        else:
            print("Authentication error. Verify your Token.")
            print(response_auth.status_code, response_auth.text)
            return
    except Exception as e:
        print(f"Error connecting: {e}")
        return

    try:
        posicao_url = f"{base_url}/Posicao"
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Download started!")
        response = session.get(posicao_url)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Download finished!")

        if response.status_code == 200:
            data = response.json()
            return data

        else:
            print(f"Error getting positions: {response.status_code}")

    except Exception as e:
        print(f"Error during execution: {e}")


def get_buses_positions_summary(buses_positions):
    reference_time = buses_positions.get("hr", "N/A")
    lines = buses_positions.get("l", [])  # 'l' contém a lista de lines e veículos
    total_vehicles = sum([len(line.get("vs", [])) for line in lines])
    return reference_time, total_vehicles
