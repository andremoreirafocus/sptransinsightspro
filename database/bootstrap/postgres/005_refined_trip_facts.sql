\connect sptrans_insights

CREATE TABLE IF NOT EXISTS refined.dim_time (
    time_key     INTEGER     NOT NULL,
    date_actual  DATE        NOT NULL,
    month        SMALLINT    NOT NULL,
    day          SMALLINT    NOT NULL,
    hour_of_day  SMALLINT    NOT NULL,
    weekday      SMALLINT    NOT NULL,
    is_weekend   BOOLEAN     NOT NULL,
    PRIMARY KEY (time_key)
);

CREATE INDEX IF NOT EXISTS idx_dim_time_date_actual
    ON refined.dim_time (date_actual);

CREATE INDEX IF NOT EXISTS idx_dim_time_weekday_hour
    ON refined.dim_time (weekday, hour_of_day);

CREATE TABLE IF NOT EXISTS refined.trip_facts (
    trip_id                  TEXT NOT NULL,
    vehicle_id               INTEGER NOT NULL,
    route_id                 TEXT NOT NULL,
    direction                SMALLINT NOT NULL,
    started_at               TIMESTAMPTZ NOT NULL,
    ended_at                 TIMESTAMPTZ NOT NULL,
    duration_seconds         INTEGER,
    duration                 INTERVAL,
    is_circular              BOOLEAN,
    distance_meters          DOUBLE PRECISION,
    avg_speed_kmh            DOUBLE PRECISION,
    started_at_time_dim_key  INTEGER NOT NULL,
    ended_at_time_dim_key    INTEGER NOT NULL,
    logic_date               TIMESTAMPTZ NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (started_at, vehicle_id, trip_id)
) PARTITION BY RANGE (started_at);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM partman.part_config
        WHERE parent_table = 'refined.trip_facts'
    ) THEN
        PERFORM partman.create_parent(
            p_parent_table := 'refined.trip_facts',
            p_control      := 'started_at',
            p_interval     := '1 day',
            p_premake      := 7
        );
    END IF;
END $$;

UPDATE partman.part_config
SET retention            = '90 days',
    retention_keep_table = 'f'
WHERE parent_table = 'refined.trip_facts';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM partman.part_config
        WHERE parent_table = 'refined.trip_facts'
    ) THEN
        PERFORM partman.run_maintenance('refined.trip_facts');
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_trip_facts_route_id
    ON refined.trip_facts (route_id);

CREATE INDEX IF NOT EXISTS idx_trip_facts_direction
    ON refined.trip_facts (direction);

CREATE INDEX IF NOT EXISTS idx_trip_facts_route_direction_started
    ON refined.trip_facts (route_id, direction, started_at);

CREATE INDEX IF NOT EXISTS idx_trip_facts_logic_date
    ON refined.trip_facts (logic_date);

CREATE INDEX IF NOT EXISTS idx_trip_facts_started_at_time_dim_key
    ON refined.trip_facts (started_at_time_dim_key);

CREATE INDEX IF NOT EXISTS idx_trip_facts_ended_at_time_dim_key
    ON refined.trip_facts (ended_at_time_dim_key);
