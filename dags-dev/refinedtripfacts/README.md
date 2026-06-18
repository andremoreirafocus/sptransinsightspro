## Objetivo deste subprojeto

Construir a tabela fato analítica `refined.trip_facts` a partir das viagens finalizadas produzidas pelo `refinedfinishedtrips`, derivando atributos analíticos para suportar métricas operacionais avançadas em Metabase.

A implementação final é feita via a DAG `refinedtripfacts-v1` do Airflow.
O desenvolvimento é feito em `dags-dev/refinedtripfacts/`. As configurações são carregadas de forma automática via `pipeline_configurator`, de acordo com o ambiente de execução, seja produção (Airflow) ou desenvolvimento local.

## O que este subprojeto faz

- **Fase 1 — Medição de entradas**: conta as viagens finalizadas em `refined.finished_trips` para o `logic_date` acionado via Dataset; aborta imediatamente se nenhuma viagem for encontrada (violação do contrato upstream).
- **Fase 2 — Provisionamento de dim_time**: garante que `refined.dim_time` cobre contiguamente todas as horas do intervalo real das viagens do batch (derivado de `MIN(trip_start_time)` / `MAX(trip_end_time)`), usando `ON CONFLICT DO NOTHING` (idempotente e seguro para re-execução).
- **Fase 3 — Criação de trip_facts**: executa a derivação e inserção em `refined.trip_facts` via SQL set-based (`INSERT … SELECT … ON CONFLICT DO NOTHING`). `route_id` e `direction` são derivados de `trip_id`; `duration` é calculado como `INTERVAL`; as chaves de dimensão `*_time_dim_key` são calculadas em fuso horário `America/Sao_Paulo`.
- **Fase 4 — Verificação de dados persistidos**: lê de volta a tabela `trip_facts` para medir a contagem real de registros persistidos, a cobertura da dimensão de tempo e violações de domínio de valores. Esta etapa é um read-back independente — diferente dos outros pipelines, a transformação é um SQL set-based direto no DB engine, então a única forma de validar o resultado é ler o que foi escrito.
- **Fase 5 — Validação de qualidade de dados**: avalia os três checks (completude, cobertura de dim_time, domínio de valores) e produz um veredicto consolidado. Nenhum check aborta o pipeline — são métricas de governança.
- **Fase 6 — Relatório de qualidade**: persiste o relatório estruturado no bucket de metadata e emite o evento `quality_report_metrics`.

## Integração com Airflow Datasets

**Inlet** — acionado pelo Dataset `finished_trips_ready` publicado pelo `refinedfinishedtrips`. O payload consumido carrega:
```json
{"logical_date_string": "2026-06-08T15:00:00+00:00"}
```

Este pipeline não possui outlet.

## Relatório de qualidade e observabilidade

O pipeline executa três verificações de qualidade de dados após a persistência, todas não-abortivas:

- **completeness**: compara a contagem de registros lidos de volta de `refined.trip_facts` com a contagem de entradas em `refined.finished_trips` para o `logic_date`. Taxa de perda: `(finished_trips_read − persisted_facts) / finished_trips_read`. WARN acima de 1%, FAIL acima de 5%.
- **dim_time_coverage**: conta chaves de dimensão em `trip_facts` sem correspondência em `dim_time` (`NOT EXISTS` join). Ausência de `FOREIGN KEY` é intencional — portabilidade Redshift (ver Hard Constraints no plano). Qualquer chave sem cobertura resulta em FAIL.
- **value_domain**: conta violações de domínio: `duration_seconds < 0`, `distance_meters < 0`, `started_at > ended_at`, `avg_speed_kmh NOT BETWEEN 0 AND avg_speed_kmh_max`. Qualquer violação resulta em FAIL.

O status final é o pior dos três checks: PASS < WARN < FAIL.

O relatório final também inclui, em `details.artifacts.column_lineage`, a linhagem declarada das colunas persistidas em `refined.trip_facts`, validada contra o contrato real de saída. Se houver divergência, o artefato registra `drift_detected: true` — sem interromper a execução.

### Taxonomia de eventos

#### Eventos do orquestrador

| Evento | Quando | Conteúdo relevante |
|---|---|---|
| `execution_started` | Início da execução | `execution_id`, `correlation_id` |
| `execution_finished` | Execução concluída com sucesso | `execution_id`, `status` |
| `execution_aborted` | Qualquer fase falha e interrompe | `execution_id`, `status`, `metadata.phase` |
| `execution_phase_metrics` | Ao final de toda execução | Duração e status de cada fase em `metadata.phase_metrics` |
| `quality_report_metrics` | Após geração do relatório | Métricas de volume e qualidade em `metadata` |
| `config_load_started` / `config_load_succeeded` | Fase de carregamento de configuração | — |
| `input_trips_measurement_started` / `input_trips_measurement_succeeded` | Fase de medição de entradas | `finished_trips_read` em `_succeeded` |
| `dim_time_provisioning_started` / `dim_time_provisioning_succeeded` | Fase de provisionamento | `rows_ensured` em `_succeeded` |
| `trip_facts_creation_started` / `trip_facts_creation_succeeded` | Fase de criação | `facts_derived`, `inserted_rows`, `skipped_rows` em `_succeeded` |
| `trip_facts_verification_started` / `trip_facts_verification_succeeded` | Fase de verificação (read-back) | métricas de verificação em `_succeeded` |
| `data_quality_validation_started` / `data_quality_validation_succeeded` | Fase de validação de qualidade | veredicto dos três checks em `_succeeded` |
| `quality_report_started` / `quality_report_succeeded` | Fase de relatório | `quality_report_path` em `_succeeded` |

#### Eventos de serviço

Emitidos pelo serviço antes de levantar exceção (o orquestrador apenas roteia):

| Evento | Quando |
|---|---|
| `input_trips_measurement_failed` | Falha de DB na medição de entradas |
| `dim_time_provisioning_failed` | Falha de DB no provisionamento |
| `trip_facts_creation_failed` | Falha de DB na criação de fatos |
| `persisted_facts_measurement_failed` | Falha de DB na verificação (read-back) |
| `quality_report_failed` | Falha ao salvar o relatório de qualidade |

### Observabilidade (stack Loki + Grafana)

A observabilidade é baseada em logging estruturado: todos os eventos são emitidos em JSON com os campos `service`, `event`, `status`, `execution_id` e `correlation_id`. No Airflow, os logs são coletados pelo Promtail e enviados ao Loki. Todas as queries seguem o padrão:

```
{service="airflow_tasks"} | json | service_extracted="refinedtripfacts" | event="<evento>"
```

## Pré-requisitos

- Tabelas `refined.trip_facts` e `refined.dim_time` existentes no banco `sptrans_insights` (criadas via `automation/bootstrap_postgres.sh`)
- Tabela `refined.finished_trips` existente e populada com `logic_date` não nulo (`TIMESTAMPTZ`)
- Dataset `finished_trips_ready` sendo emitido pelo `refinedfinishedtrips` com payload `{"logical_date_string": "..."}`
- Bucket de metadata no MinIO para armazenamento dos relatórios de qualidade
- Arquivo `.env` com as credenciais necessárias (template em `.env.example`)

## Configurações

As configurações são centralizadas no módulo `pipeline_configurator` e expostas como um objeto canônico com:
- `general`
- `connections`

### Local/dev

- `general` vem do arquivo `dags-dev/refinedtripfacts/config/refinedtripfacts_general.json`
- `.env` em `dags-dev/refinedtripfacts/.env` é usado apenas para credenciais de conexão

Credenciais esperadas no `.env`:
```
DB_HOST=<db_hostname>
DB_PORT=<PORT>
DB_DATABASE=<dbname>
DB_USER=<user>
DB_PASSWORD=<password>
DB_SSLMODE="prefer"
MINIO_ENDPOINT=<hostname:port>
ACCESS_KEY=<key>
SECRET_KEY=<secret>
```

Chaves esperadas em `general`:
```json
{
  "tables": {
    "finished_trips_table_name": "refined.finished_trips",
    "trip_facts_table_name": "refined.trip_facts",
    "dim_time_table_name": "refined.dim_time"
  },
  "quality": {
    "completeness_loss_rate_warn_threshold": 0.01,
    "completeness_loss_rate_fail_threshold": 0.05,
    "avg_speed_kmh_max": 120.0
  }
}
```

## Testes

```bash
# Testes unitários (sem banco, execução padrão)
pytest tests/ --ignore=tests/integration

# Testes de integração (requerem PostgreSQL real, opt-in)
# Pré-requisito (uma vez): criar o banco test_sptrans
bash tests/integration/bootstrap_test_db.sh

pytest tests/integration -m integration
```

A conexão dos testes de integração é configurada em `tests/integration/.env` (baseie-se em `.env.example`). O banco alvo deve ser `test_sptrans` — nunca o banco de produção.

## Instruções para instalação

- `cd dags-dev`
- `python3 -m venv .venv`
- `source .venv/bin/activate`
- `pip install -r requirements.txt`

## Configurações de Banco de dados

Antes da execução, as tabelas `refined.trip_facts` e `refined.dim_time` devem existir no banco `sptrans_insights`.

O caminho operacional recomendado é executar o bootstrap PostgreSQL do projeto:

```bash
./automation/bootstrap_postgres.sh
```

Este script aplica os arquivos SQL em `database/bootstrap/postgres/`, incluindo `005_refined_trip_facts.sql`.

### Schema e particionamento de referência da tabela `refined.trip_facts`

```sql
CREATE TABLE refined.trip_facts (
    trip_id                  TEXT NOT NULL,
    vehicle_id               INTEGER NOT NULL,
    route_id                 TEXT NOT NULL,
    direction                SMALLINT NOT NULL,
    started_at               TIMESTAMPTZ NOT NULL,
    ended_at                 TIMESTAMPTZ NOT NULL,
    duration_seconds         INTEGER,
    duration                 INTERVAL,
    is_circular              BOOLEAN,
    distance_meters          DOUBLE PRECISION,
    avg_speed_kmh            DOUBLE PRECISION,
    started_at_time_dim_key  INTEGER NOT NULL,
    ended_at_time_dim_key    INTEGER NOT NULL,
    logic_date               TIMESTAMPTZ NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (started_at, vehicle_id, trip_id)
) PARTITION BY RANGE (started_at);
```

Particionamento: diário por `started_at`, retenção de 90 dias via `pg_partman`.

### Schema de referência da tabela `refined.dim_time`

```sql
CREATE TABLE refined.dim_time (
    time_key     INTEGER  NOT NULL,
    date_actual  DATE     NOT NULL,
    month        SMALLINT NOT NULL,
    day          SMALLINT NOT NULL,
    hour_of_day  SMALLINT NOT NULL,
    weekday      SMALLINT NOT NULL,
    is_weekend   BOOLEAN  NOT NULL,
    PRIMARY KEY (time_key)
);
```

`time_key` formato: `YYYYMMDDHH` no fuso horário `America/Sao_Paulo`. Exemplo: `2026060814` = 8 de junho de 2026 às 14h no horário de São Paulo.

## Airflow (produção)

No Airflow, as configurações e credenciais são gerenciadas via Variables e Connections:
- Variable `refinedtripfacts_general` (JSON)
- Credenciais via Connections (MinIO e Postgres)

Antes da execução da DAG no Airflow, as tabelas devem estar criadas conforme instruções acima.


## Dicionário de dados

**Granularidade**: uma linha por viagem finalizada. Chave primária: `(started_at, vehicle_id, trip_id)`.

| Coluna | Tipo | Origem | Regra de derivação |
|---|---|---|---|
| `trip_id` | TEXT NOT NULL | `refined.finished_trips.trip_id` | Propagado diretamente |
| `vehicle_id` | INTEGER NOT NULL | `refined.finished_trips.vehicle_id` | Propagado diretamente |
| `route_id` | TEXT NOT NULL | `trip_id` | `LEFT(trip_id, LENGTH(trip_id) - 2)` — remove os dois últimos caracteres que codificam o sentido |
| `direction` | SMALLINT NOT NULL | `trip_id` | `CASE RIGHT(trip_id, 1) WHEN '0' THEN 1 WHEN '1' THEN 2 END` |
| `started_at` | TIMESTAMPTZ NOT NULL | `refined.finished_trips.trip_start_time` | Renomeação |
| `ended_at` | TIMESTAMPTZ NOT NULL | `refined.finished_trips.trip_end_time` | Renomeação |
| `duration_seconds` | INTEGER | `refined.finished_trips.duration_seconds` | Propagado diretamente |
| `duration` | INTERVAL | `duration_seconds` | `make_interval(secs => duration_seconds)` — para legibilidade em dashboards |
| `is_circular` | BOOLEAN | `refined.finished_trips.is_circular` | Propagado diretamente |
| `distance_meters` | DOUBLE PRECISION | `refined.finished_trips.distance_meters` | Propagado diretamente. Semântica herdada: **proxy linear entre os terminais para viagens não-circulares**; **distância ponto-a-ponto para viagens circulares**. O campo `is_circular` qualifica a interpretação. |
| `avg_speed_kmh` | DOUBLE PRECISION | `refined.finished_trips.avg_speed_kmh` | Propagado diretamente |
| `started_at_time_dim_key` | INTEGER NOT NULL | `started_at` | `to_char(trip_start_time AT TIME ZONE 'America/Sao_Paulo', 'YYYYMMDDHH24')::int` |
| `ended_at_time_dim_key` | INTEGER NOT NULL | `ended_at` | `to_char(trip_end_time AT TIME ZONE 'America/Sao_Paulo', 'YYYYMMDDHH24')::int` |
| `logic_date` | TIMESTAMPTZ NOT NULL | `refined.finished_trips.logic_date` | Propagado diretamente — identifica o batch de ingestão que originou a viagem |
| `created_at` | TIMESTAMPTZ NOT NULL | sistema | `DEFAULT NOW()` — timestamp de inserção na camada refined |

**`trip_id` como chave de join para `refined.trip_details`**: `trip_id` é a chave de join consistente com `refined.trip_details`. A integridade referencial é estabelecida upstream no join de enriquecimento do `transformlivedata` (`merge on trip_id`) e não é revalidada pelo `refinedtripfacts`. O join `trip_facts ↔ trip_details` é uma responsabilidade de consumo (dashboards), e a completude de `trip_details` é gerenciada pelo pipeline GTFS.

**`started_at_time_dim_key` / `ended_at_time_dim_key`**: referenciam `refined.dim_time.time_key` **sem `FOREIGN KEY`**. A cobertura é garantida pela ordem de carga (provisão antes da criação) e **verificada em tempo de execução pelo check `dim_time_coverage`** — não por uma constraint de banco. Isso é deliberado para portabilidade de engine: uma migração futura para Redshift não enforçaria a FK (Redshift trata FK/PK/UNIQUE como informacionais), e a camada de qualidade por read-back é SQL puro portável.
