\connect sptrans_insights

CREATE SCHEMA to_be_processed;

CREATE TABLE IF NOT EXISTS to_be_processed.raw (
    id BIGSERIAL PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    logical_date TIMESTAMPTZ NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_processed
ON to_be_processed.raw(processed);

CREATE INDEX IF NOT EXISTS idx_raw_filename
ON to_be_processed.raw(filename);

CREATE INDEX IF NOT EXISTS idx_raw_logical_date
ON to_be_processed.raw(logical_date);

CREATE INDEX IF NOT EXISTS idx_raw_created_at
ON to_be_processed.raw(created_at);
