## Objetivo deste subprojeto
Extrair os dados de posição dos ônibus a partir da API da SPTRANS periodicamente e salvá-los na camada raq.
A implementação final é feita via um microserviço que é executado via um container Docker orquestrado pelo Docker Compose

## O que este subprojeto faz
- extrai a informação de posição dos ônibus a partir da API da SPTRANS periodicamente em um intervalo previamente definido, fazendo uso de exponential backoff em caso de falha na obtenção de dados válidos
- cria em memória um objeto JSON contendo o payload e metadados sobre a extração do dados, como o timestamp da operação e a origem do dado 
- salva este objeto em uma pasta local
- salva este mesmo objeto em uma pasta no Minio
- remove o arquivo local apos o salvamento no Minio

## Pré-requisitos
- Disponibilidade do serviço de object storage para salvamento dos dados extraídos da API da SPTrans 
- Criação do arquivo de configurações

## Configurações
API_BASE_URL = "https://api.olhovivo.sptrans.com.br/v2.1"
TOKEN =  <insira o token de acesso à API, obtido após cadastro no site da SPTrans>
INTERVALO = 120  # intervalo entre extrações subsequentes dos dados de posição de omibus em segundos 

## Para instalar os requisitos
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

Para buildar e rodar o container em standalone:
    copie o arquivo .env para .env-docker e ajuste hostname e porta adequadamente
```shell
    cd ./extractloadlivedata
    docker build -t sptrans-extractloadlivedata -f Dockerfile .
    docker run --name extractloadlivedata sptrans-extractlivedat
```
    Para comunicação com os outros containers
```shell
    docker run --name extractloadlivedata --network engenharia-dados_rede_fia sptrans-extractloadlivedata
```

No docker compose:
    Para buildar o container
```shell
        docker compose build --no-cache extractloadlivedata
    Para iniciar o container 
```shell
        docker compose up -d extractloadlivedata

## Para criar o tópico Kafka necessário ao subprojeto:
Para iniciar o Kafka:
```shell
    docker compose up -d kafka-broker zookeeper akhq
```
Para criar o tópico:
```shell
    docker exec -it kafka-broker /bin/bash
    kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic sptrans-positions;



