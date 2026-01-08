import requests
import time
from datetime import datetime
import json


def monitorar_onibus(base_url, token, intervalo=120):
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
            posicao_url = f"{base_url}/Posicao"
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Carga iniciada!")
            response = session.get(posicao_url)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Carga finalizada!")

            if response.status_code == 200:
                dados = response.json()
                file_path = "posicoes_onibus.json"
                with open(file_path, "w") as file:
                    file.write(json.dumps(dados))

                print(f"Successfully written to {file_path}")
                
                
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

        time.sleep(intervalo)
