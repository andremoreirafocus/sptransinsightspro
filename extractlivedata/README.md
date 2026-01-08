Este projeto faz download de informações da SPTRANS:
- posição dos ônibus de SP usando a API da SPTRANS

Credenciais necessárias se encontram no arquivo .env do projeto usando as seguintes variáveis:
API_BASE_URL = "https://api.olhovivo.sptrans.com.br/v2.1"
TOKEN =  <insira o seu token>
INTERVALO = 120  # 2 minutos em segundos

Para instalar os requisitos:
pip install -r requirements.txt

Kafka:
    Foi necessário mudar a porta do akhq para 28080 no docker-compose-yaml para parar de conflitar com outros componentes
    docker compose up -d kafka-broker zookeeper akhq

    Para criar o tópico:
    docker exec -it kafka-broker /bin/bash
    kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic sptrans-positions;


Para executar: 
python ./main.py


