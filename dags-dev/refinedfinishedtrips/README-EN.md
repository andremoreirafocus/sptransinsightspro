## Purpose of this subproject

Compute finished trips from the history of real-time bus positions and store their consolidated history for efficiency analysis.
The final production implementation runs through the Airflow `refinedfinishedtrips` DAG.
Development happens inside `dags-dev`, which contains the Airflow-based subprojects and speeds up local experimentation.
Configuration is loaded automatically through `pipeline_configurator`, according to the execution environment, either production (Airflow) or local development.

## What this subproject does

For each route and vehicle:
- reads real-time positions stored in the `positions` table under the `sptrans` area of the trusted bucket in the object storage service, partitioned by year, month, day, and hour, for a given analysis time window
- checks the quality of position data before processing trips by running two validations:
  - **freshness**: validates whether the most recent vehicle timestamp is within the expected staleness threshold
  - **extraction gaps**: validates whether there are no significant gaps between extraction timestamps in the recent window
- if quality checks fail: stops the pipeline, saves a quality report to the metadata bucket, and sends a webhook notification
- if quality checks produce a warning: saves a quality report to the metadata bucket, sends a webhook notification, and continues processing
- computes finished trips during the analysis time window
- checks the quality of trip extraction by running two validations over the effective extraction window, measured by the interval between the first and last `extracao_ts` in the dataset:
  - **zero trips**: warning if the effective extraction window exceeds the configured threshold and no trip is identified
  - **low trip count**: warning if the effective extraction window exceeds the configured threshold and the number of identified trips is below the expected minimum
- if a failure occurs during the trip-extraction phase: saves a failure report with the partial results already available, sends a webhook notification, and stops execution
- saves finished trips to the refined layer implemented in the low-latency analytical database for use by the visualization layer
- checks the persistence result, recording how many trips:
  - were actually inserted in this execution
  - had already been saved previously
- if a failure occurs during the persistence phase: saves a failure report with the partial results already available, sends a webhook notification, and stops execution
- at the end of every successful execution, saves a complete quality report to the metadata bucket and sends a webhook notification with the consolidated status of the three phases: positions, trip extraction, and persistence

## Trip extraction algorithm

The current algorithm was designed to produce trips with higher operational fidelity, especially in three key dimensions for analysis:
- `trip_start_time`
- `trip_end_time`
- `duration`

These fields are especially sensitive to common noise in production position data, such as:
- a bus standing still at a terminal before starting a trip
- `linha_sentido` changes at operational boundary moments
- isolated spatial samples incompatible with the real path
- processing windows that start or end in the middle of an operational cycle

For this reason, extraction does not rely solely on `linha_sentido`. The logic uses geolocation primarily to identify the beginning and end of trips, and uses `linha_sentido` only as a complementary signal when needed, especially for circular routes.

### 1. Spatial sanitization of positions

Before extracting trips, the pipeline sanitizes the sequence of positions for each route/vehicle combination.

This sanitization removes isolated spatially invalid samples characterized by:
- an intermediate point incompatible with the previous point
- also incompatible with the next point
- while the direct link between previous and next remains plausible

In practice, this removes isolated "teleport" points without discarding full sequences of data.
This step is important because a single invalid spatial point can:
- artificially anticipate a trip start
- close a trip at the wrong place
- incorrectly inflate or reduce the observed duration

As a result, final extraction becomes more stable and more faithful to the vehicle's actual movement.

### 2. Non-circular routes

For non-circular routes, the algorithm uses geographic proximity to the reference terminals:
- `first_stop`
- `last_stop`

A trip is formed when:
- the vehicle is observed near one terminal
- then moves away from that terminal
- and later arrives at the opposite terminal

The trip start is recorded as the last position still close to the departure terminal before actual movement begins.
The trip end is recorded when the vehicle enters the proximity zone of the arrival terminal.

This means detection depends on the observed spatial path, not only on `linha_sentido`.
This strategy is needed because:
- the bus may remain stopped at the terminal for several collection cycles before departure
- `linha_sentido` may change at boundary moments before or after effective movement

By using the observed spatial boundary, the algorithm directly improves:
- trip start accuracy
- trip end accuracy
- the quality of the calculated duration

### 3. Circular routes

For circular routes, the algorithm uses a combination of two boundary signals:
- proximity to the terminal/anchor
- `linha_sentido` change

After the vehicle first synchronizes with the terminal region, the algorithm detects trips considering that:
- a trip may start when leaving the terminal
- a trip may also start when `linha_sentido` changes
- a trip may end when returning to the terminal
- a trip may also end when `linha_sentido` changes

This avoids losing trips when the analysis window starts in the middle of a cycle or when the direction change happens outside the terminal area.
This behavior matters because, in circular routes:
- the processing window may not capture the departure from the terminal
- one trip may need to be closed on return to the terminal
- another may need to be segmented by an operational `linha_sentido` change

Without this combination of signals, circular trips would tend to be underdetected or have imprecise temporal boundaries.

### 4. Removing terminal idle time

Periods when the bus stays parked waiting to begin operation should not inflate trip duration.

For this reason, the algorithm:
- removes terminal dwell from the beginning of the trip
- uses as the effective start the last terminal position immediately before actual departure

This removal is applied only in terminal context, not during in-route movement.
It is necessary so that duration reflects operating time, not parked waiting time before departure.

### 5. Direction validation

The `linha_sentido` field is not used as the sole basis for discovering trips.

It is used as a complementary signal:
- mainly for segmenting circular routes
- and for validating consistency of the detected trip

During validation, the algorithm disregards boundary samples near terminals, because in those regions `linha_sentido` may change before or after actual movement due to collection granularity.

Together, these decisions make the finished-trips table more reliable for:
- operational efficiency analysis
- duration comparison between trips
- cycle and regularity evaluation
- analytical consumption in the visualization layer

## Quality reporting and notification

The pipeline produces structured reports for three phases:
- `positions`
- `trip_extraction`
- `persistence`

Failure reports preserve the partial results available up to the interruption point.
This means a failure in `trip_extraction` or `persistence` does not lose the context already computed in previous phases.

In the final report, the `trip_extraction` phase also exposes aggregated operational metrics from the execution, including:
- `trips_extracted`
- `source_sentido_discrepancies`
- `sanitization_dropped_points`
- `vehicle_line_groups_processed`
- `input_position_records`

The final report also includes, under `details.artifacts.column_lineage`, the declared lineage of the columns persisted to `refined.finished_trips`:
- `trip_id`
- `vehicle_id`
- `trip_start_time`
- `trip_end_time`
- `duration`
- `is_circular`
- `average_speed`

This lineage is validated against the real output contract of the pipeline.
If there is any divergence between declared columns and the columns actually produced/persisted, the artifact records:
- `drift_detected: true`
- `warning: "lineage drift detected"`

This lineage drift is reported as a governance artifact in the quality report and does not, by itself, interrupt execution.

The `persistence` phase of the final report directly exposes the persistence result, including:
- `added_rows`
- `previously_saved_rows`

The `summary` follows the common contract consumed by `alertservice`.
The pipeline sends a webhook for:
- failures in `positions`
- warnings in `positions`
- failures in `trip_extraction`
- failures in `persistence`
- the final consolidated report

Webhook delivery is explicitly logged, indicating whether:
- the notification was sent
- the notification was disabled
- the attempt failed

The [samples](./samples) folder contains a manually curated example of the consolidated quality report: [quality-report-refinedfinishedtrips_HHMM_uuid.json](./samples/quality-report-refinedfinishedtrips_HHMM_uuid.json).

## Prerequisites

- availability of the trusted-layer bucket, already created in the object storage service
- availability of the metadata bucket in the object storage service to store quality reports
- creation of an object storage access key registered in the configuration file with read access to the trusted-layer bucket and write access to the metadata bucket
- availability of the analytical database service, currently PostgreSQL, for storing data in the refined layer
- `alertservice` available and configured to receive quality notifications (configurable through `webhook_url`; use `"disabled"` to disable it)
- `.env` file with the required credentials
- a template is available in `.env.example`
- creation of the configuration file

## Configuration

Configuration is centralized in the `pipeline_configurator` module and exposed as a canonical object with:
- `general`
- `connections`

### Local/dev

- `general` comes from `dags-dev/refinedfinishedtrips/config/refinedfinishedtrips_general.json`
- `.env` in `dags-dev/refinedfinishedtrips/.env` is used only for connection credentials

Expected credentials in `.env`:
```bash
MINIO_ENDPOINT=<hostname:port>
ACCESS_KEY=<key>
SECRET_KEY=<secret>
DB_HOST=<db_hostname>
DB_PORT=<PORT>
DB_DATABASE=<dbname>
DB_USER=<user>
DB_PASSWORD=<password>
DB_SSLMODE="prefer"
```

Expected keys in `general`:
```json
{
  "analysis": {
    "hours_window": 3
  },
  "storage": {
    "app_folder": "sptrans",
    "trusted_bucket": "trusted",
    "metadata_bucket": "metadata",
    "quality_report_folder": "quality-reports"
  },
  "tables": {
    "positions_table_name": "positions",
    "finished_trips_table_name": "refined.finished_trips"
  },
  "quality": {
    "freshness_warn_staleness_minutes": 10,
    "freshness_fail_staleness_minutes": 30,
    "gaps_warn_gap_minutes": 5,
    "gaps_fail_gap_minutes": 15,
    "gaps_recent_window_minutes": 60,
    "trips_effective_window_threshold_minutes": 60,
    "trips_min_trips_threshold": 5
  },
  "notifications": {
    "webhook_url": "http://localhost:8000/notify"
  }
}
```

To disable `alertservice` notifications, configure:
- `notifications.webhook_url = "disabled"`

## Installation instructions

To install the requirements:
- `cd dags-dev`
- `python3 -m venv .venv`
- `source .venv/bin/activate`
- `pip install -r requirements.txt`

## Database setup required before execution

Before running this pipeline locally, the required refined-layer tables must exist in the `sptrans_insights` database.

The recommended operational path to create the required database artifacts is to run the project's PostgreSQL bootstrap:

```bash
./automation/bootstrap_postgres.sh
```

This script applies the SQL files located in `/database/bootstrap/postgres/`.

### Reference schema and partitioning for `refined.finished_trips`

The block below is kept as documentation reference for the expected table structure and partitioning setup:

```sql
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

-- Initialize partitioning
-- This creates the first few partitions based on the current time
SELECT partman.create_parent(
    p_parent_table := 'refined.finished_trips',
    p_control := 'trip_start_time',
    p_interval := '1 hour',
    p_premake := 4
);

-- Set the 24-hour automatic purge policy
UPDATE partman.part_config
SET retention = '24 hours',
    retention_keep_table = 'f'
WHERE parent_table = 'refined.finished_trips';

-- Optimized search index for Power BI
-- This supports searching for a specific route/direction
-- and narrowing it down by bus.
CREATE INDEX idx_trip_lookup
ON refined.finished_trips (trip_id, vehicle_id);

-- This creates future partitions and checks whether any are older than 24 hours to drop them
SELECT partman.run_maintenance('refined.finished_trips');

-- To verify
SELECT
    parent_table,
    control,
    partition_interval,
    retention,
    automatic_maintenance
FROM partman.part_config
WHERE parent_table = 'refined.finished_trips';

-- To check existing partitions
SELECT * FROM partman.show_partitions('refined.finished_trips');

-- To check partition usage
SELECT
    nmsp_parent.nspname AS parent_schema,
    parent.relname AS parent_table,
    child.relname AS partition_name,
    pg_size_pretty(pg_total_relation_size(child.oid)) AS total_size,
    child.reltuples::bigint AS estimated_row_count
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
WHERE parent.relname = 'finished_trips'
ORDER BY child.relname DESC;
```

### Airflow (production)

In Airflow, configuration and credentials are managed through Variables and Connections stored by Airflow itself, as listed below. Any change to this information must be made through the Airflow UI or through the command line by connecting to the Airflow webserver with `docker exec`.
- Variable `refinedfinishedtrips_general` (JSON)
- Credentials via Connections (MinIO and Postgres)

Before executing the DAG in Airflow, the required tables must already exist as described above.

## Local execution instructions

Create `dags-dev/refinedfinishedtrips/.env` based on `.env.example` and fill in all fields.
With the tables already created as described above, run:

```bash
python ./refinedfinishedtrips-v<version number>.py
```

Example:
```bash
python ./refinedfinishedtrips-v4.py
```
