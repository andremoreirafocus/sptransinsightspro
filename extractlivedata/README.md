Este projeto:
- extrai a informação de posição dos ônibus a partir da API da SPTRANS periodicamente em um intervalo previamente definido, fazendo uso de exponential backoff em caso de falha na obtenção de dados válidos
- cria em memória um objeto JSON contendo o payload e metadados sobre a extração do dados, como o timestamp da operação e a origem do dado 
- posta uma mensagem em um tópico Kafka contendo o objeto gerado a cada extração de dados da API

Configurações necessárias no arquivo .env do projeto:
API_BASE_URL = "https://api.olhovivo.sptrans.com.br/v2.1"
TOKEN =  <insira o seu token>
INTERVALO = 120  # 2 minutos em segundos
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "bus_positions"

Para instalar os requisitos:
- cd <diretorio deste subprojeto>
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

Para executar local: 
    python ./main.py

No docker compose:
    Para buildar o container
        docker compose build --no-cache extractlivedata
    Para iniciar o container 
        docker compose up -d extractlivedata

Para buildar e rodar o container em standalone:
    copie o arquivo .env para .env-docker e ajuste hostname e porta adequadamente
    cd ./extractlivedata
    docker build -t sptrans-extractlivedata -f Dockerfile .
    docker run --name extractlivedata sptrans-extractlivedat
    Para comunicação com os outros containers
    docker run --name extractlivedata --network engenharia-dados_rede_fia sptrans-extractlivedata

Kafka:
    Foi necessário mudar a porta do akhq para 28080 no docker-compose-yaml para parar de conflitar com outros componentes
    docker compose up -d kafka-broker zookeeper akhq

    Para criar o tópico:
    docker exec -it kafka-broker /bin/bash
    kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic sptrans-positions;



