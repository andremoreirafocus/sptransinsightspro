Este projeto:
- lê cada um dos arquivos extraidos gtfs do portal do desenvolvedor e que se encontarm em uma subpasta gtfs no bucker "raw" e salva no db em um schema trusted, para cada arquivo relevante, uma tabela com o mesmo nome que o arquivo ainda sem nenhuma transformação.

Configurações:
SOURCE_BUCKET = "raw"
APP_FOLDER = "gtfs"
SCHEMA=<schema for trusted layer> 
MINIO_ENDPOINT=<hostname:port>
ACCESS_KEY=<key>
SECRET_KEY=<secret>
DB_HOST=<hostname>
DB_PORT=<port>
DB_DATABASE="sptrans_insights"
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

Instruções adicionais:
Database commands:

CREATE TABLE trusted.routes (
    route_id TEXT,
    agency_id INTEGER,	
    route_short_name TEXT,
    route_long_name TEXT,
    route_type INTEGER,
    route_color TEXT,
    route_text_color TEXT
);

CREATE TABLE trusted.trips (
    trip_id TEXT,
    route_id TEXT,
    service_id TEXT,
    trip_headsign TEXT,	
    direction_id TEXT,
    shape_id TEXT
);


CREATE TABLE trusted.stops (
    stop_id INTEGER,
    stop_name TEXT,
    stop_desc TEXT,
    stop_lat  DOUBLE PRECISION,
    stop_lon  DOUBLE PRECISION
);


CREATE TABLE trusted.stop_times (
    trip_id TEXT,
    arrival_time TEXT,
    departure_time TEXT,
    stop_id INTEGER,
    stop_sequence INTEGER
);


CREATE TABLE trusted.frequencies (
    trip_id TEXT,
    start_time TEXT,
    end_time TEXT,
    headway_secs INTEGER
);


CREATE TABLE trusted.calendar (
    service_id     TEXT,
    monday         INTEGER,
    tuesday        INTEGER,
    wednesday      INTEGER,
    thursday       INTEGER,
    friday         INTEGER,
    saturday       INTEGER,
    sunday         INTEGER,
    start_date     INTEGER,
    end_date       INTEGER
);

DROP TABLE trusted.routes;
DROP TABLE trusted.trips;
DROP TABLE trusted.stops;
DROP TABLE trusted.stop_times;
DROP TABLE trusted.frequencies;
DROP TABLE trusted.calendar;

SELECT * FROM trusted.routes;
SELECT * FROM trusted.trips;
SELECT * FROM trusted.stops;
SELECT * FROM trusted.stop_times;
SELECT * FROM trusted.frequencies;
SELECT * FROM trusted.calendar;

# o comando abaixo não é necessario porque a implementacao faz um CTAS
CREATE TABLE trusted.trip_details (
        trip_id (from trips)
        first_stop_id (from stop_times)
        first_stop_name (from stops)
        first_stop_lat  (from stops)
        first_stop_lon  (from stops)
        last_stop_id (from stop_times) (null quando is_circular)
        last_stop_name (from stops)
        last_stop_lat (from stops) (null quando is_circular)
        last_stop_lon (from stops) (null quando is_circular)
        trip_linear_distance (calculated from tp_lat, tp_lon, ts_lat, ts_lon in this table)
        is_circular (from trips)
)


PESQUISAS:
# LINHA CIRCULAR:
SELECT * FROM trusted.routes
where route_id = '1012-10';

SELECT * FROM trusted.stop_times
where trip_id = '1012-10-0';

# primeira stop
SELECT * FROM trusted.stops
where stop_id = '301790';

# ultima stop
SELECT * FROM trusted.stops
where stop_id = '30003051';

# LINHA COM DOIS SENTIDOS:
SELECT * FROM trusted.routes
where route_id = '1016-10';

SELECT * FROM trusted.stop_times
where trip_id = '1016-10-0' union ALL
SELECT * FROM trusted.stop_times
where trip_id = '1016-10-1'
order by 1,5;

# primeira stop
SELECT * FROM trusted.stops
where stop_id = '301790';

# ultima stop
SELECT * FROM trusted.stops
where stop_id = '30003051';

SELECT * FROM trusted.frequencies
where trip_id in ('1016-10-0','1016-10-1');

# Criação da tabela trip_details
CREATE TABLE IF NOT EXISTS trusted.trip_details AS
WITH trip_extremes AS (
    SELECT 
        trip_id,
        MAX(CASE WHEN start_rank = 1 THEN stop_id END) AS first_stop_id,
        MAX(CASE WHEN end_rank = 1 THEN stop_id END) AS last_stop_id
    FROM (
        SELECT 
            trip_id, stop_id, 
            ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY stop_sequence ASC) as start_rank,
            ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY stop_sequence DESC) as end_rank
        FROM trusted.stop_times
    ) ranked_data
    GROUP BY trip_id
),
trip_metrics AS (
    SELECT 
        te.trip_id,
        te.first_stop_id,
        s1.stop_lat AS first_stop_lat,
        s1.stop_lon AS first_stop_lon,
        te.last_stop_id,
        s2.stop_lat AS last_stop_lat,
        s2.stop_lon AS last_stop_lon,
        ROUND(SQRT(POW(s2.stop_lat - s1.stop_lat, 2) + POW(s2.stop_lon - s1.stop_lon, 2)) * 106428) AS trip_linear_distance
    FROM trip_extremes te
    JOIN trusted.stops s1 ON te.first_stop_id = s1.stop_id
    JOIN trusted.stops s2 ON te.last_stop_id = s2.stop_id
)
SELECT 
    *,
    (trip_linear_distance = 0) AS is_circular
FROM trip_metrics;