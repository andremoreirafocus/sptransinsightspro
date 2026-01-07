import requests
import time
import json
from datetime import datetime
from dotenv import dotenv_values

# --- CONFIGURAÇÕES ---
TOKEN = "4f758764150e72262cfd9cb08a09426b43137b50383952bfef6cccf523cc44cb"
BASE_URL = "https://api.olhovivo.sptrans.com.br/v2.1"
INTERVALO = 120  # 2 minutos em segundos


def monitorar_onibus(token, base_url):
    session = requests.Session()

    auth_url = f"{base_url}/Login/Autenticar?token={token}"
    try:
        response_auth = session.post(auth_url)
        if response_auth.status_code == 200 and response_auth.text.lower() == "true":
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Autenticado com sucesso!")
        else:
            print("Erro na autenticação. Verifique seu Token.")
            print(response_auth.status_code, response_auth.text)
            return
    except Exception as e:
        print(f"Erro de conexão: {e}")
        return

    while True:
        try:
            # Endpoint /Posicao retorna todos os veículos com posição atualizada
            # Para uma linha específica, use: /Posicao/Linha?codigoLinha={ID}
            posicao_url = f"{BASE_URL}/Posicao"
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Carga iniciada!")
            response = session.get(posicao_url)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Carga finalizada!")

            if response.status_code == 200:
                dados = response.json()
                # print(dados)

                horario_ref = dados.get("hr", "N/A")
                veiculos = dados.get("l", [])  # 'l' contém a lista de linhas e veículos

                total_veiculos = sum([len(linha.get("vs", [])) for linha in veiculos])

                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] Ref SPTrans: {horario_ref} | Veículos Ativos: {total_veiculos}"
                )

            else:
                print(f"Erro ao buscar posições: {response.status_code}")

        except Exception as e:
            print(f"Erro durante a execução: {e}")

        time.sleep(INTERVALO)


def main():
    config = dotenv_values(".env")
    TOKEN = config.get("TOKEN")
    BASE_URL = config.get("BASE_URL")
    monitorar_onibus(token=TOKEN, base_url=BASE_URL)


if __name__ == "__main__":
    main()
