\connect sptrans_insights

CREATE SCHEMA IF NOT EXISTS refined;

CREATE TABLE IF NOT EXISTS refined.latest_positions (
    id BIGSERIAL PRIMARY KEY,
    veiculo_ts TIMESTAMPTZ,
    veiculo_id INTEGER,
    veiculo_lat DOUBLE PRECISION,
    veiculo_long DOUBLE PRECISION,
    linha_lt TEXT,
    linha_sentido INTEGER,
    trip_id TEXT
);
