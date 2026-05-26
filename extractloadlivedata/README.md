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
- usa `metadata.extracted_at` como fonte única para o timestamp do nome do artefato bruto e do particionamento em storage; `payload.hr` permanece apenas como referência informacional da origem devido a drifts neste campo da resposta da API
- na pasta [samples](./samples) há exemplos curados manualmente do artefato bruto salvo pelo serviço:
  - [posicoes_onibus-YYYYMMDDHHmm.json](./samples/posicoes_onibus-YYYYMMDDHHmm.json)
  - [posicoes_onibus-YYYYMMDDHHmm.json.zst](./samples/posicoes_onibus-YYYYMMDDHHmm.json.zst)
- mantém arquivos locais pendentes de salvamento no object storage quando este está indisponível, tentando novamente nas próximas execuções e removendo o arquivo local após a persistência ter sido bem-sucedida
- registra em um banco de dados uma requisição de processamento para cada arquivo salvo na camada raw a ser processado pelo pipeline. O banco de dados em questão é hospedado na instância utilizada pelo Airflow. Caso a criação do do registro falhe, o mesmo continua salvo localmente até que a operação seja concluída com sucesso. Caso o Airflow esteja indisponível, ao retornar ao funcionamento, a DAG orquestradora identifica os registros de arquivos pendentes de transformação e dispara a DAG de transformação para cada arquivo, um por vez e na ordem de criação dos arquivos de posição dos ônibus, garantindo uma entrega ordenada das posições ao longo do tempo.
- alternativamente, pode disparar diretamente a DAG de transformação via API do Airflow, sem criar registro no banco, dependendo da configuração

## Observabilidade: Structured Logging

O serviço implementa observabilidade de nível de produção com **logs estruturados em JSON**, **métricas por fase** e **rastreamento de linhagem de dados** via correlation ID.

### Logs Estruturados
- **Formato**: JSON com campos `timestamp`, `level`, `service`, `component`, `event`, `status`, `message`, `metadata`
- **Saída**: stdout para ingestão por Loki
- **Taxonomia**: eventos estruturados em [src/domain/events.py](./src/domain/events.py)
- **Status**: enum controlado (`STARTED`, `SUCCEEDED`, `FAILED`, `RETRY`, `SKIPPED`)

### Rastreamento de Execução (execution_id)
Cada execução recebe um `execution_id` em **formato ISO 8601** (timestamp UTC de início):
- Exemplo: `"2026-05-13T15:30:15.987654+00:00"`
- Correlaciona todos os logs de uma execução no Loki

### Métricas por Fase (Phase Instrumentation)
O serviço rastreia **tentativas, sucessos e falhas** em três fases:

1. **Fase de Extração (extract)**
   - Tenta extrair posições da API
   - Captura `logical_datetime` de `buses_positions["metadata"]["extracted_at"]` (timestamp do dado)
   - Salva localmente e passa para a próxima fase

2. **Fase de Salvamento (save)**
   - Persiste arquivos locais pendentes no MinIO
   - Cada arquivo salvo é registrado no banco como requisição de processamento
   - Rastreia tempo total de salvamento

3. **Fase de Notificação (notify)**
   - Dispara DAG de transformação (via Airflow ou banco de dados)
   - Rastreia sucesso/falha de cada invocação
   - Agregação final de métricas

### Evento de Métricas Finais (execution_metrics_final)
Emitido ao final de cada execução com estrutura Prometheus-compatível:

```json
{
  "event": "execution_metrics_final",
  "status": "SUCCEEDED",
  "execution_id": "2026-05-13T15:30:15.987654+00:00",
  "correlation_id": "2026-05-13T15:30:45.123456+00:00",
  "metadata": {
    "phase_metrics": {
      "extract": {"attempted": 1, "succeeded": 1, "failed": 0, "duration": 3.21},
      "save": {"attempted": 3, "succeeded": 2, "failed": 1, "duration": 7.89},
      "notify": {"attempted": 3, "succeeded": 3, "failed": 0, "duration": 1.35}
    },
    "items_total": 7,
    "items_failed": 1,
    "retries_seen": 4,
    "execution_seconds": 12.45
  }
}
```

### Rastreamento de Linhagem de Dados (Correlation ID)
Cada operação é marcada com `correlation_id = logical_datetime` (timestamp do dado):
- Permite rastrear "todo o processamento do dado extraído em 2026-05-13T15:30:45.123456Z" através de todas as pipelines
- Habilita queries no Loki: `{correlation_id="2026-05-13T15:30:45.123456Z"}` para ver todas as operações deste dado
- Propaga de extractloadlivedata → transformlivedata → refinedfinishedtrips para lineage completo

### Taxonomia de Eventos

Todos os eventos são emitidos via `{service="extractloadlivedata"}` (stream direto do container Docker).

Eventos do **scheduler** (main.py):

| Evento | Quando |
|---|---|
| `scheduler_config_loaded` | Configuração carregada com sucesso |
| `scheduler_started` | Scheduler iniciado |
| `scheduler_tick_started` / `scheduler_tick_completed` | Início e fim de cada tick do APScheduler |
| `scheduler_stopped` | Scheduler parado (SIGTERM/SIGINT) |
| `scheduler_shutdown_completed` | Shutdown do scheduler concluído |
| `cli_dev_mode_requested` | Execução iniciada em modo `dev` (execução única) |
| `cli_invalid_parameter` | Parâmetro CLI inválido |

Eventos do **orquestrador** (extractloadlivedata.py):

| Evento | Quando | Conteúdo relevante |
|---|---|---|
| `config_validation_succeeded` / `config_validation_failed` | Validação da configuração | `error_message` em caso de falha |
| `notification_engine_selected` | Motor de notificação determinado | `metadata.engine` |
| `execution_started` | Início de uma execução | `execution_id` |
| `extract_positions_started` / `extract_positions_succeeded` | Fase de extração | — |
| `pending_storage_scan_succeeded` / `pending_storage_scan_failed` | Varredura do buffer local | `metadata.pending_count` |
| `pending_storage_detected` / `pending_storage_multiple_files_detected` | Arquivos pendentes detectados | `metadata.pending_count` |
| `pending_storage_file_started` / `pending_storage_file_succeeded` / `pending_storage_file_failed` | Processamento de cada arquivo pendente | `metadata.filename` |
| `notification_dispatch_started` / `notification_dispatch_succeeded` / `notification_dispatch_failed` | Despacho de notificação | `metadata.filename` |
| `notification_metrics_invalid` | Métricas de notificação inconsistentes | — |
| `execution_metrics_final` | Ao final de toda execução | `metadata.phase_metrics`, `metadata.items_total`, `metadata.items_failed`, `metadata.retries_seen`, `metadata.execution_seconds` |
| `execution_summary_emitted` | Resumo enviado ao alertservice | — |
| `execution_completed` | Execução encerrada sem falhas fatais | `metadata.items_total`, `metadata.items_failed`, `metadata.retries_seen` |
| `execution_failed_non_recoverable` | Execução encerrada com falha não recuperável | `metadata.items_failed`, `metadata.failure_phase` |

Eventos de serviço — **Extração** (extract_buses_positions.py):

| Evento | Quando | Conteúdo relevante |
|---|---|---|
| `api_authentication_successful` / `api_authentication_failed` | Autenticação na API SPTrans | `error_message` em falha |
| `api_get_started` / `api_get_successful` / `api_get_failed` | Chamada GET à API | `metadata.attempt` |
| `extract_positions_succeeded_after_retries` | Extração bem-sucedida após retries | `metadata.retries` |
| `extract_positions_failed` | Extração falhou em todas as tentativas | `error_type`, `error_message` |
| `summarize_extracted_positions_succeeded` | Resumo das posições extraídas gerado | `metadata.total_vehicles` |
| `metadata_validation_failed` | Estrutura do payload inválida | `metadata.payload_sample` |

Eventos de serviço — **Salvamento local** (save_load_bus_positions.py):

| Evento | Quando | Conteúdo relevante |
|---|---|---|
| `local_storage_persist_started` | Início do salvamento no volume local | `metadata.filename` |
| `local_storage_compression_succeeded` | Compressão Zstandard concluída | `metadata.filename` |
| `local_storage_persist_succeeded` | Arquivo salvo no volume local | `metadata.filename` |
| `local_storage_persist_failed` | Falha ao salvar no volume local | `metadata.filename`, `error_type` |

Eventos de serviço — **Object storage** (save_load_bus_positions.py):

| Evento | Quando | Conteúdo relevante |
|---|---|---|
| `object_storage_compression_started` / `object_storage_compression_succeeded` | Compressão antes do upload | `metadata.filename` |
| `object_storage_persist_started` / `object_storage_persist_succeeded` / `object_storage_persist_failed` | Persistência no MinIO | `metadata.bucket`, `metadata.object_name` |
| `object_storage_list_failed` | Falha ao listar objetos pendentes no MinIO | `error_type`, `error_message` |
| `remove_pending_storage_file_succeeded` / `remove_pending_storage_file_failed` | Remoção do arquivo local após upload | `metadata.filename` |

Eventos de serviço — **Banco de dados** (save_processing_requests.py):

| Evento | Quando | Conteúdo relevante |
|---|---|---|
| `db_storage_persist_started` / `db_storage_persist_succeeded` / `db_storage_persist_failed` | Criação do registro de processamento no PostgreSQL | `metadata.filename`, `metadata.table` |

Eventos de serviço — **Disparo via Airflow API** (trigger_airflow.py):

| Evento | Quando | Conteúdo relevante |
|---|---|---|
| `get_utc_logical_date_succeeded` / `get_utc_logical_date_failed` | Cálculo da data lógica UTC | `metadata.logical_date` |

### Dashboard Grafana

O dashboard está em [`observability/grafana/provisioning/dashboards/extractloadlivedata.json`](./observability/grafana/provisioning/dashboards/extractloadlivedata.json) e é provisionado automaticamente pelo Grafana. Utiliza Loki como datasource. Todas as queries usam o stream `{service="extractloadlivedata"}`.

Janela padrão: `now-1h`. Atualização: `30s`.

![Dashboard extractloadlivedata](extractloadlivedata_dashboard.png)

| Painel | Tipo | O que mostra | Evento Loki / campo |
|---|---|---|---|
| Executions | Timeseries (pontos) | `execution_completed` (verde), `execution_failed_non_recoverable` (vermelho), erros e avisos ao longo do tempo | `execution_completed`, `execution_failed_non_recoverable`, level `ERROR`, level `WARNING` — `count_over_time [5m]` |
| Errors (last 1h) | Stat (vermelho se ≥ 1) | Total de logs com `level="ERROR"` na última hora | `count_over_time [1h]` |
| Warnings (last 1h) | Stat (laranja se ≥ 1) | Total de logs com `level="WARNING"` na última hora | `count_over_time [1h]` |
| Execution time (s) | Timeseries | Duração média por fase: `total`, `extract`, `save`, `notify` | `execution_metrics_final` — `metadata.execution_seconds` e `metadata.phase_durations.<fase>` via `avg_over_time [5m]` |
| Recent failures | Logs | Stream filtrado por `level="ERROR"` em ordem decrescente | — |
| Log stream | Logs | Todos os eventos do serviço em ordem decrescente | — |

### Regras de Alerta

As regras estão em `observability/loki/rules/fake/extractloadlivedata-alerts.yaml` e são avaliadas a cada minuto:

| Alerta | Severidade | Condição | Janela |
|---|---|---|---|
| `ServiceFailed` | critical | Evento `execution_failed_non_recoverable` detectado | 5m |
| `ServiceWarningThreshold` | warning | `execution_completed` com `metadata.retries_seen > 0` | 5m |

## Execution Reporting (Alertservice)
- Escopo: o serviço publica **resumo de execução** para alertservice; não há persistência de artefato JSON de relatório.
- Contrato de resumo enviado:
  - `contract_version`, `pipeline`, `execution_id`, `status`
  - `items_total`, `items_failed`, `retries`, `acceptance_rate`
  - `generated_at_utc` (timestamp UTC da geração do resumo)
  - `failure_phase`, `failure_message` (apenas em `FAIL`)
  - `quality_report_path` com valor `"null"` por compatibilidade de contrato.
- Enum de status de execução:
  - `PASS`: nenhuma falha, zero retries
  - `WARN`: nenhuma falha mas com retries detectados
  - `FAIL`: uma ou mais fases falharam
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
- Arquivo `.env` com as configurações necessárias
- Um template está disponível em `.env.example`
- Criação de schema e tabela no banco de dados para armazenamento dos reequests de processamento de arquivos de posição de ônibus extraídos da API

O caminho operacional recomendado para criação desses artefatos de banco é executar o bootstrap PostgreSQL do Airflow:

```bash
./automation/bootstrap_airflow_postgres.sh
```

Esse script aplica os arquivos SQL localizados em `/database/bootstrap/airflow_postgres/`.

### Schema de referência da tabela `to_be_processed.raw`

O bloco abaixo é mantido como referência documental da estrutura esperada da tabela:

```sql
CREATE SCHEMA to_be_processed;

CREATE TABLE to_be_processed.raw (
    id BIGSERIAL PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    logical_date TIMESTAMPTZ NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_raw_processed ON to_be_processed.raw(processed);
CREATE INDEX idx_raw_filename ON to_be_processed.raw(filename);
CREATE INDEX idx_raw_logical_date ON to_be_processed.raw(logical_date);
CREATE INDEX idx_raw_created_at ON to_be_processed.raw(created_at);
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
- `tests/test_extractloadlivedata_orchestrator.py`: roteamento do orquestrador entre `processing_requests` e `airflow`, validação de configuração, e testes de orquestração com alertservice integrado.
- `tests/test_alertservice.py`: construção de payload, envio de alertas com webhook enabled/disabled, e comportamento não-bloqueante em falhas.
- `tests/test_reporting.py`: construção de sumário de execução com contrato validado (success e failure paths).
- `tests/test_sql_db_v2.py`: contratos de persistência, seleção e atualização com engine injetado.
- `tests/test_object_storage.py`: leitura, listagem e escrita no object storage com cliente injetado.


## Para instalar os requisitos
- cd <diretorio deste subprojeto>
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

## Para executar: 
Localmente:
Crie `extractloadlivedata/.env` com base em `.env.example` preenchendo todos os campos:

```shell
    python ./main.py
```

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
