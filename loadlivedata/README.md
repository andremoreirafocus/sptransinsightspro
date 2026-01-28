Este projeto:
- consome de um tópico Kafka as posiçõesdos onibus produzidas pelo processo extractlivedata obtidas da API de posicao da SPTrans
- salva os dados consumidos em um bucket no Minio com particionamento por ano,mes e dia para processamento pelo transformlivedata

Configurações necessárias no arquivo .env do projeto:
INTERVALO = 120  # 2 minutos em segundos
KAFKA_BROKER = <kafka_broker>
KAFKA_TOPIC = <kafka_topic>
MINIO_ENDPOINT= <hostname:porta>
ACCESS_KEY= <key>
SECRET_KEY= <secret>
RAW_BUCKET_NAME = <bucket_name>
APP_FOLDER = <folder>

Para instalar os requisitos:
- cd <diretorio deste subprojeto>
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

Para executar: 
python ./main.py

Para buildar o container
cd ./loadlivedata
docker build -t sptrans-loadlivedata -f Dockerfile .

in docker compose:
docker compose up -d loadlivedata

in standalone mode:
docker run --name loadlivedata --network engenharia-dados_rede_fia sptrans-loadlivedata

docker run --name loadlivedata sptrans-extractlivedat

Instruções adicionais:
Kafka:
    Foi necessário mudar a porta do akhq para 28080 no docker-compose-yaml para parar de conflitar com outros componentes
    docker compose up -d kafka-broker zookeeper akhq

    Para criar o tópico:
    docker exec -it kafka-broker /bin/bash
    kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic sptrans-positions;

Minio:
    Criar o bucket raw
    Criar o access key e informar access key e secret key no .env do projeto
