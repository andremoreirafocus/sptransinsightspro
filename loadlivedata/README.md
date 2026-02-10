## Objetivo deste subprojeto
Salvar os dados de posição dos ônibus extraídos periodicamente através da API da SPTRANS 
A implementação final é feita via um microserviço que é executado via um container Docker orquestrado pelo Docker Compose

## O que este subprojeto faz
- consome de um tópico Kafka as posiçõesdos onibus produzidas pelo processo extractlivedata obtidas da API de posicao da SPTrans
- salva os dados consumidos em um bucket no Minio com particionamento por ano,mes e dia para processamento pelo transformlivedata

## Pré-requisitos
- Disponibilidade do serviço Kafka e um tópico para consumo dos dados extraídos da API da SPTrans pelo subprojeto extractlivedata
- Disponibilidade de um bucket da camada raw previamente criado no serviço de storage, atualmente o Minio
- Criação de uma chave de acesso ao Minio cadastrada no arquivo de configurações com acesso de escrita no bucket da camada raw
- Disponibilidade do microserviço extractlivedata para extração dos dados da API e publicação dos dados extraídos em um tópico Kafka para consumo pelo laodlivedata
- Criação do arquivo de configurações

## Configurações
INTERVALO = 120  # 2 minutos em segundos
KAFKA_BROKER = <kafka_broker>
KAFKA_TOPIC = <kafka_topic>
MINIO_ENDPOINT= <hostname:porta>
ACCESS_KEY= <key>
SECRET_KEY= <secret>
RAW_BUCKET_NAME = <bucket_name>
APP_FOLDER = <folder>

## Para instalar os requisitos:
- cd <diretorio deste subprojeto>
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

## Para executar: 
Localmente:
```shell
    python ./main.py
```

    Se o arquivo .env não existir na raiz do projeto, crie-o com as variáveis enumeradas acima

Para buildar o container
```shell
    cd ./loadlivedata
    docker build -t sptrans-loadlivedata -f Dockerfile .

Para buildar e rodar o container em standalone:
    copie o arquivo .env para .env-docker e ajuste hostname e porta adequadamente
```shell
    cd ./loadlivedata
    docker build -t sptrans-loadlivedata -f Dockerfile .
    docker run --name loadlivedata sptrans-extractlivedat
    Para comunicação com os outros containers
    docker run --name loadlivedata --network engenharia-dados_rede_fia sptrans-loadlivedata
```

No docker compose:
    Para buildar o container
```shell
        docker compose build --no-cache loadlivedata
```
    Para iniciar o container 
```shell
        docker compose up -d loadlivedata
```

## Para criar o tópico Kafka necessário ao subprojeto:
Para iniciar o Kafka:
```shell
    docker compose up -d kafka-broker zookeeper akhq
```

Para criar o tópico:
```shell
    docker exec -it kafka-broker /bin/bash
    kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic sptrans-positions;
```
