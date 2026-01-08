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
        # Endpoint /Posicao retorna todos os veículos com posição atualizada
        # Para uma linha específica, use: /Posicao/Linha?codigoLinha={ID}
        posicao_url = f"{base_url}/Posicao"
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Carga iniciada!")
        response = session.get(posicao_url)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Carga finalizada!")

        if response.status_code == 200:
            dados = response.json()
            return dados

        else:
            print(f"Error getting positions: {response.status_code}")

    except Exception as e:
        print(f"Error during execution: {e}")


def get_buses_positions_summary(buses_positions):
    horario_ref = buses_positions.get("hr", "N/A")
    veiculos = buses_positions.get("l", [])  # 'l' contém a lista de linhas e veículos
    total_veiculos = sum([len(linha.get("vs", [])) for linha in veiculos])
    return horario_ref, total_veiculos
