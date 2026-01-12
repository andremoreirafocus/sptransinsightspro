Este projeto faz:
- lê cada um dos arquivos em um prefixo de uma pasta raw no minio

Configurações:
RAW_BUCKET_NAME = "raw"
APP_FOLDER = "gtfs"
TRUSTED_BUCKET_NAME = "trusted"


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

Para instalar os requisitos:
pip install -r requirements.txt

Para executar: 
python ./main.py
