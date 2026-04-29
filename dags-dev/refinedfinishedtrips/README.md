## Objetivo deste subprojeto
Calcular as viagens finalizadas a partir do histórico de posições instantâneas dos ônibus e armazena o seu histórico consolidado para análise de eficiência.
A implementação final é feita via a DAG refinedfinishedtrips do Airflow.
O desenvolvimento é feito em uma pasta dag-dev que contem cada um dos subprojetos implementados via Airflow, aumentando a agilidade durante a experimentação.
As configurações são carregadas de forma automática via `pipeline_configurator`, de acordo com o ambiente de execução, seja produção (Airflow) ou desenvolvimento local.

## O que este subprojeto faz
Para cada linha e veículo: 
- lê as posições instantâneas armazenadas na tabela de posições armazendas sptrans no bucket da camada trusted no serviço de object storage, particionados por ano, mes, dia e hora, correspondentes a um período de tempo de análise
- verifica a qualidade dos dados de posição antes de processar as viagens, executando duas verificações:
  - **freshness**: valida se o timestamp mais recente dos veículos está dentro do limiar de atualização esperado
  - **gaps de extração**: valida se não há lacunas significativas entre os timestamps de extração na janela recente
- em caso de falha nas verificações de qualidade: interrompe o pipeline, salva um relatório de qualidade no bucket de metadata e notifica via webhook
- em caso de aviso nas verificações de qualidade: salva um relatório de qualidade no bucket de metadata, notifica via webhook e continua o processamento
- calcula as viagens finalizadas durante este período de tempo de análise 
- salva as viagens finalizadas na camada refined implementada no banco de dados analítico de baixa latência, para consumo da camada de visualização

## Pré-requisitos
- Disponibilidade do buckets da camada trusted, previamente criado no serviço de object storage
- Disponibilidade do bucket da camada metadata no serviço de object storage para armazenamento dos relatórios de qualidade
- Criação de uma chave de acesso ao serviço de object storage cadastrada no arquivo de configurações com acesso de leitura ao bucket na camada trusted e escrita ao bucket na camada metadata
- Disponibilidade do serviço de banco de dados analítico, atualmente o PostgreSQL, para armazenamento dos dados na camada refined
- alertservice disponível e configurado para receber notificações de qualidade (configurável via `webhook_url`; use `"disabled"` para desativar)
- Arquivo `.env` com as credenciais necessárias
- Um template está disponível em `.env.example`
- Criação do arquivo de configurações

## Configurações
As configurações são centralizadas no módulo `pipeline_configurator` e expostas como um objeto canônico com:
- `general`
- `connections`

### Local/dev
- `general` vem do arquivo `dags-dev/refinedfinishedtrips/config/refinedfinishedtrips_general.json`
- `.env` em `dags-dev/refinedfinishedtrips/.env` é usado apenas para credenciais de conexão

Credenciais esperadas no `.env`:
MINIO_ENDPOINT=<hostname:port>
ACCESS_KEY=<key>
SECRET_KEY=<secret>
DB_HOST=<db_hostname>
DB_PORT=<PORT>
DB_DATABASE=<dbname>
DB_USER=<user>
DB_PASSWORD=<password>
DB_SSLMODE="prefer"

Chaves esperadas em `general`
```json
{
  "analysis": {
    "hours_window": 3
  },
  "storage": {
    "app_folder": "sptrans",
    "trusted_bucket": "trusted",
    "metadata_bucket": "metadata",
    "quality_report_folder": "quality-reports"
  },
  "tables": {
    "positions_table_name": "positions",
    "finished_trips_table_name": "refined.finished_trips"
  },
  "quality": {
    "freshness_warn_staleness_minutes": 10,
    "freshness_fail_staleness_minutes": 30,
    "gaps_warn_gap_minutes": 5,
    "gaps_fail_gap_minutes": 15,
    "gaps_recent_window_minutes": 60
  },
  "notifications": {
    "webhook_url": "disabled"
  }
}
```

### Airflow (produção)
No Airflow, as configurações e credenciais são gerenciadas utilzando-se os recursos de Variables e Connections que são armazenadas pelo próprio Airflow, conforme listado a seguir. Qualquer alteração nessas informações deve ser feitas via UI do Airflow ou via linha de comando conectando-se ao webserver do Airflow via comando docker exec.
- Variable `refinedfinishedtrips_general` (JSON)
- Credenciais via Connections (MinIO e Postgres)

## Instruções para instalação
Para instalar os requisitos:
- cd dags-dev
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

## Instruções para execução em modo local
Crie `dags-dev/refinedfinishedtrips/.env` com base em `.env.example` preenchendo todos os campos.
Criar tabelas conforme instruções abaixo:

```shell
python ./refinedfinishedtrips-v4.py
```

## Configurações de Banco de dados que devem ser feitas antes da execução:
## Para criar as tabelas e índices necessários ao subprojeto:

Database commands:

```
docker exec -it postgres bash
psql -U postgres -W
```

```sql
CREATE DATABASE sptrans_insights;
```

\c sptrans_insights

Com particonamemto:

```sql
CREATE SCHEMA partman;
CREATE EXTENSION pg_partman SCHEMA partman;

CREATE SCHEMA refined;



CREATE TABLE refined.finished_trips (
    trip_id TEXT,
    vehicle_id INTEGER,
    trip_start_time TIMESTAMPTZ NOT NULL,
    trip_end_time TIMESTAMPTZ,
    duration INTERVAL,
    is_circular BOOLEAN,
    average_speed DOUBLE PRECISION,
    PRIMARY KEY (trip_start_time, vehicle_id, trip_id)
) PARTITION BY RANGE (trip_start_time);

-- 2. Initialize partitioning
-- This creates the first few partitions based on the current time
SELECT partman.create_parent(
    p_parent_table := 'refined.finished_trips',
    p_control := 'trip_start_time',
    p_interval := '1 hour',
    p_premake := 4
);

-- 3. Set the 24-hour "Automatic Purge" policy
UPDATE partman.part_config 
SET retention = '24 hours', 
    retention_keep_table = 'f' 
WHERE parent_table = 'refined.finished_trips';

-- Optimized Search Index for PowerBI
-- This supports searching for a specific route/direction 
-- and narrowing it down by bus.
CREATE INDEX idx_trip_lookup 
ON refined.finished_trips (trip_id, vehicle_id);

-- This will create future partitions and check if any are > 24h old to drop
SELECT partman.run_maintenance('refined.finished_trips');

-- to verify
SELECT 
    parent_table, 
    control, 
    partition_interval, 
    retention,
    automatic_maintenance
FROM partman.part_config
WHERE parent_table = 'refined.finished_trips';

-- To check existing partitions
SELECT * FROM partman.show_partitions('refined.finished_trips');


-- to check partitions usage
SELECT 
    nmsp_parent.nspname AS parent_schema,
    parent.relname AS parent_table,
    child.relname AS partition_name,
    pg_size_pretty(pg_total_relation_size(child.oid)) AS total_size,
    child.reltuples::bigint AS estimated_row_count
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
WHERE parent.relname = 'finished_trips'
ORDER BY child.relname DESC;
```

Deprecated
CREATE TABLE refined.finished_trips (
    trip_id TEXT,             -- e.g., '101A_0'
    vehicle_id INTEGER,       -- e.g., 505
    trip_start_time TIMESTAMPTZ,
    trip_end_time TIMESTAMPTZ,
    duration INTERVAL,
    is_circular BOOLEAN,
    average_speed DOUBLE PRECISION,
    -- This combination is guaranteed unique by your bus logic
    PRIMARY KEY (trip_start_time, vehicle_id, trip_id)
);



#Tabela usada apenas em testes de algoritmo experimental
```sql
CREATE TABLE trusted.ongoing_trips (
    id BIGSERIAL PRIMARY KEY,
    trip_id TEXT,
    vehicle_id INTEGER,
    trip_start_time TIMESTAMPTZ,
    trip_end_time TIMESTAMPTZ,
    duration INTERVAL,
    is_circular BOOLEAN,
    average_speed DOUBLE PRECISION
);
