## Objetivo deste subprojeto
Extrair os dados de posição dos ônibus a partir da API da SPTRANS periodicamente e salvá-los na camada raq.
A implementação final é feita via um microserviço que é executado via um container Docker orquestrado pelo Docker Compose

## O que este subprojeto faz
- extrai periodicamente posições de ônibus da API da SPTrans em um intervalo configurável; em caso de falhas ou payload inválido, a operação é reexecutada com backoff exponencial
- valida a estrutura mínima do payload recebido e registra métricas de referência (horário e total de veículos)
- cria em memória um objeto JSON com `metadata` (origem, timestamp e total de veículos) e `payload` original
- salva o JSON localmente em um volume configurado
- persiste o JSON no MinIO na camada raw, em uma pasta por data, podendo salvar comprimido em Zstandard ou em JSON puro
- mantém arquivos locais pendentes de salvamento no object storage quando este está indisponível, tentando novamente nas próximas execuções e removendo o arquivo local após a persistência tesr sido bem-sucedida
- registra em um banco de dados uma requisição de processamento para cada arquivo salvo na camada raw a ser processado pelo pipeline. O banco de dados em questão é hospedado na instância utilizada pelo Airflow


## Pré-requisitos
- Disponibilidade do serviço de object storage para salvamento dos dados extraídos da API da SPTrans 
- Criação do arquivo de configurações
- Criação de schema e tabela no banco de dados para armazenamento dos reequests de processamento de arquivos de posição de ônibus extraídos da API

```sql
CREATE DATABASE sptrans_insights;

\c sptrans_insights
-- First, ensure the schema exists
CREATE SCHEMA to_be_processed;

-- Create the table
CREATE TABLE IF NOT EXISTS to_be_processed.raw (
    id BIGSERIAL PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    logical_date TIMESTAMPTZ NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

-- Create index on processed column for efficient filtering of unprocessed requests
CREATE INDEX IF NOT EXISTS idx_raw_processed ON to_be_processed.raw(processed);

-- Create index on filename column for efficient file lookups
CREATE INDEX IF NOT EXISTS idx_raw_filename ON to_be_processed.raw(filename);

-- Create index on logical_date column for efficient date-based queries
CREATE INDEX IF NOT EXISTS idx_raw_logical_date ON to_be_processed.raw(logical_date);

-- Create index on created_at for ordering queries
CREATE INDEX IF NOT EXISTS idx_raw_created_at ON to_be_processed.raw(created_at);
```

## Configurações
API_BASE_URL = "https://api.olhovivo.sptrans.com.br/v2.1"
TOKEN =  <insira o token de acesso à API, obtido após cadastro no site da SPTrans>
EXTRACTION_INTERVAL_SECONDS = 120  # intervalo entre extrações subsequentes dos dados de posição de omibus em segundos 
API_MAX_RETRIES = 4   # numero de retries do get na api com backoff exponencial 
STORAGE_MAX_RETRIES = 0 # numero de retries da escrita no object storage com backoff exponencial alem do que a bblioteca implementa
MINIO_ENDPOINT="localhost:9000"
ACCESS_KEY="datalake"
SECRET_KEY="datalake"
SOURCE_BUCKET = "raw"
APP_FOLDER = "sptrans"
INGEST_BUFFER_PATH = "../ingest_buffer"  # pasta aonde os arquivos são salvos no volume local
DATA_COMPRESSION_ON_SAVE = "true"  # habilita a compressão de arquivos ao salvar local e na camda raw


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


