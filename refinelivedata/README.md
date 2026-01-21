Este projeto faz:
- extrai as posicoes para uma linha e veiculo para um ano, mes e dia;
- calcula as trips para uma linha e veiculo para um ano, mes e dia;
- salva as trips calculadas

Configurações:
# TABLE_NAME=<table_name_including_schema> # where data will be written
FINISHED_TRIPS_TABLE_NAME=<table_name_for_finished_trips_including_schema_in_refined>
POSITIONS_TABLE_NAME=<table_name_for_positions_in_trusted>
LATEST_POSITIONS_TABLE_NAME=<table_name_for_positions_in_refined>
DB_HOST=<db_hostname>
DB_PORT=<PORT>
DB_DATABASE=<dbname>
DB_USER=<user>
DB_PASSWORD=<password>
DB_SSLMODE="prefer"

Para instalar os requisitos:
- cd <diretorio deste subprojeto>
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

Para executar: 
python ./main.py

CREATE SCHEMA refined;

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

CREATE TABLE refined.latest_positions AS
WITH latest_snapshot AS (
    -- 1. Captura o timestamp exato do último lote de extração
    SELECT MAX(extracao_ts) AS max_ts
    FROM trusted.positions
)
-- 2. Projeta os dados e calcula o trip_id com o mapeamento 1->0 e 2->1
SELECT 
    p.veiculo_id, 
    p.veiculo_lat,  
    p.veiculo_long, 
    p.linha_lt, 
    p.linha_sentido,
    -- Concatenação da linha com o sentido mapeado
    p.linha_lt || '-' || (
        CASE 
            WHEN p.linha_sentido = 1 THEN '0' 
            WHEN p.linha_sentido = 2 THEN '1' 
            ELSE NULL -- Garante integridade para valores inesperados
        END
    ) AS trip_id
FROM trusted.positions p
JOIN latest_snapshot ls ON p.extracao_ts = ls.max_ts;

CREATE TABLE trusted.finished_trips (
    id BIGSERIAL PRIMARY KEY,
    trip_id TEXT,
    vehicle_id INTEGER,
    trip_start_time TIMESTAMPTZ,
    trip_end_time TIMESTAMPTZ,
    duration INTERVAL,
    is_circular BOOLEAN,
    average_speed DOUBLE PRECISION
);

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
