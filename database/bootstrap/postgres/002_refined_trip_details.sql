\connect sptrans_insights

CREATE SCHEMA IF NOT EXISTS refined;

CREATE TABLE IF NOT EXISTS refined.trip_details (
    id BIGSERIAL PRIMARY KEY,
    trip_id TEXT,
    first_stop_id INTEGER,
    first_stop_name TEXT,
    first_stop_lat DOUBLE PRECISION,
    first_stop_lon DOUBLE PRECISION,
    last_stop_id INTEGER,
    last_stop_name TEXT,
    last_stop_lat DOUBLE PRECISION,
    last_stop_lon DOUBLE PRECISION,
    trip_linear_distance DOUBLE PRECISION,
    is_circular BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_trip_lookup
ON refined.trip_details (trip_id);
