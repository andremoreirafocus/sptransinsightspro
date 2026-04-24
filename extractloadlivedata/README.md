## Objetivo deste subprojeto
Extrair os dados de posição dos ônibus a partir da API da SPTRANS periodicamente e salvá-los na camada raw.
A implementação final é feita via um microserviço que é executado via um container Docker orquestrado pelo Docker Compose.
Como o principal objetivo do projeto é extrair informações sobre as viagens concluídas, a partir da extração periódica das posições instantâneas de todos os ônibus via api e esta informação não pode ser obtida em nenhum outro momento, a robustez do serviço é vital para o atingimento do onjetivo do projeto, foi desenhada uma arquitetura que permitisse que os dados fossem extraídos mesmo que o serviço de object storage e o orquestrador estivessem indisponíveis.
O serviço utiliza uma robusta biblioteca de agendamento de tarefas "in-process" chamada Advanced Python Scheduler que tem funcionalidades e proporciona agendamento com drift extremamente pequeno.
Para tal, todos os arquivos que contem as posições de ônibus extraídas da API são primeiramente salvos em um volume local e persistente gerenciado por este microserviço e, somente em seguida, são salvos no object storage e, em caso de sucesso, removidos do volume local.
Para suportar esta possibilidade de falha e recuperação dos dados, a cada arquivo salvo no object storage é criado um registro em uma tabela de itens a serem processados pelo orquestrador que através de uma DAG orquestardora, verifica periodicamente se há nesta tabela algum regsitro de arquivo de posições a ser processado e, em caso positivo, inicia uma outra DAG de execução da transformação dos dados de posição contidos no arquivo.
Esta arquitetura suporta a falha do obsjetc storage, do orquestador e do banco de dados do orquestrador, aonde os registros de arquivos a serem processados são salvos.
Embora esta não seja a melhor opção para resiliência completa do fluxo, há também a possibilidade de, ao invés de registrar a solicitação no banco de dados para a DAG orquestradora, disparar diretamente a execução da DAG de transformação via API do Airflow. Essa escolha é configurável por variável de ambiente e permite alternar entre um fluxo baseado em fila persistida e um fluxo baseado em disparo direto.

## O que este subprojeto faz
- extrai periodicamente posições de ônibus da API da SPTrans em um intervalo configurável; em caso de falhas ou payload inválido, a operação é reexecutada com backoff exponencial
- valida a estrutura mínima do payload recebido e registra métricas de referência (horário e total de veículos)
- cria em memória um objeto JSON com `metadata` (origem, timestamp e total de veículos) e `payload` original
- salva o JSON localmente em um volume configurado
- persiste o JSON no MinIO na camada raw, em uma pasta por data, podendo salvar comprimido em Zstandard ou em JSON puro
- mantém arquivos locais pendentes de salvamento no object storage quando este está indisponível, tentando novamente nas próximas execuções e removendo o arquivo local após a persistência ter sido bem-sucedida
- registra em um banco de dados uma requisição de processamento para cada arquivo salvo na camada raw a ser processado pelo pipeline. O banco de dados em questão é hospedado na instância utilizada pelo Airflow. Caso a criação do do registro falhe, o mesmo continua salvo localmente até que a operação seja concluída com sucesso. Caso o Airflow esteja indisponível, ao retornar ao funcionamento, a DAG orquestradora identifica os registros de arquivos pendentes de transformação e dispara a DAG de transformação para cada arquivo, um por vez e na ordem de criação dos arquivos de posição dos ônibus, garantindo uma entrega ordenada das posições ao longo do tempo.
- alternativamente, pode disparar diretamente a DAG de transformação via API do Airflow, sem criar registro no banco, dependendo da configuração

## Execution Reporting (Alertservice)
- Escopo: o serviço publica **somente resumo de execução** para alertservice; não há persistência de artefato JSON de relatório.
- Contrato de resumo enviado:
  - `contract_version`, `pipeline`, `execution_id`, `status`
  - `items_total`, `items_failed`, `retries`
  - `failure_phase`, `failure_message` (apenas em `FAIL`)
  - `quality_report_path` com valor `"null"` por compatibilidade de contrato.
- Enum de fase de falha (orquestração):
  - `positions_download`
  - `local_ingest_buffer_save_positions`
  - `save_positions_to_raw`
  - `ingest_notification`
  - fallback: `unknown`
- Mensagens fixas por fase:
  - `positions_download`: `[SEVERE] non recoverable api get failed`
  - `local_ingest_buffer_save_positions`: `[SEVERE] non recoverable save to local buffer failed`
  - `save_positions_to_raw`: `save to raw storage failed`
  - `ingest_notification`: `ingest notification failed`
  - `unknown`: `ingest execution failed`
- Regra de severidade: apenas `positions_download` e `local_ingest_buffer_save_positions` recebem prefixo `[SEVERE] non recoverable `.
- Regra de envio webhook:
  - webhook ausente/`disabled`/`none`/`null` -> envio é pulado com log informativo.
  - erro de envio não interrompe o serviço (com log de erro).


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
EXTRACTION_INTERVAL_SECONDS = 120  # intervalo entre extrações subsequentes dos dados em segundos
API_BASE_URL = "https://api.olhovivo.sptrans.com.br/v2.1"  # URL base da API da SPTrans
TOKEN =  <insira o token de acesso à API, obtido após cadastro no site da SPTrans>  # token de autenticação da API
API_MAX_RETRIES = 4  # número de retries do GET na API com backoff exponencial
INGEST_BUFFER_PATH = "../ingest_buffer"  # pasta local para arquivos temporários
DATA_COMPRESSION_ON_SAVE = "true"  # habilita compressão ao salvar localmente e na camada raw
PROCESSING_REQUESTS_CACHE_DIR = "../.diskcache_pending_processing_requests"  # cache para requests pendentes de processamento
SOURCE_BUCKET = "raw"  # bucket de destino na camada raw
APP_FOLDER = "sptrans"  # pasta base do app dentro do bucket
STORAGE_MAX_RETRIES = 0  # retries de escrita no object storage com backoff exponencial
RAW_EVENTS_TABLE_NAME = "to_be_processed.raw"  # tabela de requests pendentes (schema.tabela)
NOTIFICATION_ENGINE = "processing_requests"  # define o mecanismo de notificação: "processing_requests" (banco) ou "airflow" (API)
MINIO_ENDPOINT="localhost:9000"  # endpoint do MinIO
ACCESS_KEY="datalake"  # access key do MinIO
SECRET_KEY="datalake"  # secret key do MinIO
DB_HOST="localhost"  # host do banco de dados
DB_PORT=5432  # porta do banco de dados
DB_DATABASE="sptrans_insights"  # nome do banco de dados
DB_USER="airflow"  # usuário do banco de dados
DB_PASSWORD="airflow"  # senha do banco de dados
DB_SSLMODE="prefer"  # modo SSL da conexão com o banco
#Legacy env variables
AIRFLOW_USER = "ingest_service"  # usuário para autenticação na API do Airflow
AIRFLOW_PASSWORD = "ingest_password"  # senha para autenticação na API do Airflow
AIRFLOW_WEBSERVER = "localhost"  # hostname do webserver do Airflow
AIRFLOW_DAG_NAME = "transformlivedata-v5"  # DAG alvo para invocação via API
INVOKATIONS_CACHE_DIR = "../.diskcache_pending_invocations"  # cache para invocações pendentes do Airflow

## Testes unitários
Os testes são focados em comportamento relevante e invariantes de negócio, usando injeção de dependências e fakes (sem monkeypatch), para garantir isolamento das integrações externas. A cobertura atual inclui:
- `tests/test_extract_buses_positions.py`: validações de payload, autenticação e fluxo de retries na extração.
- `tests/test_save_load_bus_positions.py`: validação de estrutura, compressão, leitura de arquivos, persistência com retries, remoção de arquivos locais e filtros de pendências.
- `tests/test_save_processing_requests.py`: criação e disparo de requests de processamento com cache e persistência no banco.
- `tests/test_trigger_airflow.py`: criação e disparo de invocações do Airflow via HTTP e cache.
- `tests/test_extractloadlivedata_orchestrator.py`: roteamento do orquestrador entre `processing_requests` e `airflow`, e validação de configuração.
- `tests/test_sql_db_v2.py`: contratos de persistência, seleção e atualização com engine injetado.
- `tests/test_object_storage.py`: leitura, listagem e escrita no object storage com cliente injetado.


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
