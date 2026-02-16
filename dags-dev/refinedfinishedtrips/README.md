## Objetivo deste subprojeto
Calcular as viagens finalizadas a partir do histórico de posições instantâneas dos ônibus e armazena o seu histórico consolidado para análise de eficiência.
A implementação final é feita via a DAG gtfs do Airflow.
O desenvolvimento é feito em uma pasta dag-dev que contem cada um dos subprojetos implementados via Airflow, aumentando a agilidade durante a experimentação.
As configurações são carregadas de forma automática - via arquivo config.py - de acordo com o ambiente de execução, seja produção, via Airflow, ou desenvolvimento, local.

## O que este subprojeto faz
Para cada linha e veículo: 
- lê as posições instantâneas armazenadas na tabela de posições armazendas sptrans no bucket da camada trusted no serviço de object storage, particionados por ano, mes, dia e hora, correspondentes a um período de tempo de análise
- calcula as viagens finalizadas durante este período de tempo de análise 
- salva as viagens finalizadas na camada refined implementada no banco de dados analítico de baixa latência, para consumo da camada de visualização

## Pré-requisitos
- Disponibilidade do buckets da camada trusted, previamente criado no serviço de object storage
- Criação de uma chave de acesso ao serviço de object storage cadastrada no arquivo de configurações com acesso de leitura ao bucket na camada trusted
- Disponibilidade do serviço de banco de dados analítico, atualmente o PostgreSQL, para armazenamento dos dados na camada refined
- Criação do arquivo de configurações

## Configurações
ANALYSIS_HOURS_WINDOW=<numero_de_horas_analise_viagens_finalizadas>
APP_FOLDER="sptrans"
TRUSTED_BUCKET = "trusted"
POSITIONS_TABLE_NAME="positions"
FINISHED_TRIPS_TABLE_NAME="refined.finished_trips"
APP_FOLDER="sptrans"
MINIO_ENDPOINT=<hostname:port>
ACCESS_KEY=<key>
SECRET_KEY=<secret>
DB_HOST=<db_hostname>
DB_PORT=<PORT>
DB_DATABASE=<dbname>
DB_USER=<user>
DB_PASSWORD=<password>
DB_SSLMODE="prefer"

## Instruções para instalação
Para instalar os requisitos:
- cd dags-dev
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

## Instruções para execução em modo local
Criar tabelas conforme instruções abaixo
python ./refinedfinishedtrips-v2.py

Se o arquivo .env não existir na raiz do projeto, crie-o com as variáveis enumeradas acima

## Configurações de Banco de dados que devem ser feitas antes da execução:
## Para criar as tabelas e índices necessários ao subprojeto:

Database commands:

docker exec -it postgres bash
psql -U postgres -W

```sql
CREATE DATABASE sptrans_insights;
```

\c sptrans_insights

```sql
CREATE SCHEMA refined;

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

-- Optimized Search Index for PowerBI
-- This supports searching for a specific route/direction 
-- and narrowing it down by bus.
CREATE INDEX idx_trip_lookup 
ON refined.finished_trips (trip_id, vehicle_id);

```

#Deprecated table creation command
CREATE TABLE refined.finished_trips (
    id BIGSERIAL PRIMARY KEY,
    trip_id TEXT,
    vehicle_id INTEGER,
    trip_start_time TIMESTAMPTZ,
    trip_end_time TIMESTAMPTZ,
    duration INTERVAL,
    is_circular BOOLEAN,
    average_speed DOUBLE PRECISION
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
