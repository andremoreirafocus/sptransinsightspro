\connect sptrans_insights

CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman WITH SCHEMA partman;

CREATE SCHEMA IF NOT EXISTS refined;

CREATE TABLE IF NOT EXISTS refined.finished_trips (
    trip_id TEXT,
    vehicle_id INTEGER,
    trip_start_time TIMESTAMPTZ NOT NULL,
    trip_end_time TIMESTAMPTZ,
    duration INTERVAL,
    is_circular BOOLEAN,
    average_speed DOUBLE PRECISION,
    PRIMARY KEY (trip_start_time, vehicle_id, trip_id)
) PARTITION BY RANGE (trip_start_time);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM partman.part_config
        WHERE parent_table = 'refined.finished_trips'
    ) THEN
        PERFORM partman.create_parent(
            p_parent_table := 'refined.finished_trips',
            p_control := 'trip_start_time',
            p_interval := '1 hour',
            p_premake := 4
        );
    END IF;
END $$;

UPDATE partman.part_config
SET retention = '24 hours',
    retention_keep_table = 'f'
WHERE parent_table = 'refined.finished_trips';

CREATE INDEX IF NOT EXISTS idx_trip_lookup
ON refined.finished_trips (trip_id, vehicle_id);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM partman.part_config
        WHERE parent_table = 'refined.finished_trips'
    ) THEN
        PERFORM partman.run_maintenance('refined.finished_trips');
    END IF;
END $$;
