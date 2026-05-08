\connect sptrans_insights

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

SELECT partman.create_parent(
    p_parent_table := 'refined.finished_trips',
    p_control := 'trip_start_time',
    p_interval := '1 hour',
    p_premake := 4
);

UPDATE partman.part_config
SET retention = '24 hours',
    retention_keep_table = 'f'
WHERE parent_table = 'refined.finished_trips';

CREATE INDEX idx_trip_lookup
ON refined.finished_trips (trip_id, vehicle_id);

SELECT partman.run_maintenance('refined.finished_trips');
