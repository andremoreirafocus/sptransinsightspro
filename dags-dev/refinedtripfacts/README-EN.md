## Purpose of this subproject

Build the analytical fact table `refined.trip_facts` from the finished trips produced by `refinedfinishedtrips`, deriving analytical attributes to support advanced operational metrics in Metabase.

The final production implementation runs through the Airflow `refinedtripfacts-v1` DAG.
Development happens inside `dags-dev/refinedtripfacts/`. Configuration is loaded automatically through `pipeline_configurator`, according to the execution environment, either production (Airflow) or local development.

## What this subproject does

- **Phase 1 — Input measurement**: counts finished trips in `refined.finished_trips` for the triggered `logic_date`; aborts immediately if no trips are found (upstream contract violation).
- **Phase 2 — dim_time provisioning**: ensures that `refined.dim_time` contiguously covers every hour in the real trip span of the batch (derived from `MIN(trip_start_time)` / `MAX(trip_end_time)`), using `ON CONFLICT DO NOTHING` (idempotent and safe for re-execution).
- **Phase 3 — trip_facts creation**: executes the derivation and insertion into `refined.trip_facts` via a set-based SQL statement (`INSERT … SELECT … ON CONFLICT DO NOTHING`). `route_id` and `direction` are derived from `trip_id`; `duration` is computed as an `INTERVAL`; the dimension keys `*_time_dim_key` are computed in `America/Sao_Paulo` timezone.
- **Phase 4 — Persisted facts verification**: reads back `trip_facts` to measure the actual count of persisted records, dim_time coverage, and value-domain violations. This is an independent read-back — unlike other pipelines, the transformation is a set-based SQL executed directly in the DB engine, so the only way to validate the output is to read what was written.
- **Phase 5 — Data quality validation**: evaluates the three checks (completeness, dim_time coverage, value domain) and produces a consolidated verdict. No check aborts the pipeline — they are governance metrics.
- **Phase 6 — Quality report**: persists the structured report to the metadata bucket and emits the `quality_report_metrics` event.

## Airflow Dataset integration

**Inlet** — triggered by the Dataset `finished_trips_ready` published by `refinedfinishedtrips`. The consumed payload carries:
```json
{"logical_date_string": "2026-06-08T15:00:00+00:00"}
```

This pipeline has no outlet.

## Quality reporting and observability

The pipeline runs three non-aborting data quality checks after persistence:

- **completeness**: compares the count of records read back from `refined.trip_facts` against the count of entries in `refined.finished_trips` for the `logic_date`. Loss rate: `(finished_trips_read − persisted_facts) / finished_trips_read`. WARN above 1%, FAIL above 5%.
- **dim_time_coverage**: counts dimension keys in `trip_facts` with no match in `dim_time` (`NOT EXISTS` join). No `FOREIGN KEY` is used — intentional for Redshift portability (see Hard Constraints in the plan). Any uncovered key results in FAIL.
- **value_domain**: counts domain violations: `duration_seconds < 0`, `distance_meters < 0`, `started_at > ended_at`, `avg_speed_kmh NOT BETWEEN 0 AND avg_speed_kmh_max`. Any violation results in FAIL.

The overall status is the worst of the three checks: PASS < WARN < FAIL.

The final report also includes, under `details.artifacts.column_lineage`, the declared lineage of columns persisted to `refined.trip_facts`, validated against the real output contract. Any divergence records `drift_detected: true` — without interrupting execution.

### Event taxonomy

#### Orchestrator events

| Event | When | Relevant content |
|---|---|---|
| `execution_started` | Execution begins | `execution_id`, `correlation_id` |
| `execution_finished` | Execution completed successfully | `execution_id`, `status` |
| `execution_aborted` | Any phase fails and stops execution | `execution_id`, `status`, `metadata.phase` |
| `execution_phase_metrics` | At the end of every execution | Duration and status of each phase in `metadata.phase_metrics` |
| `quality_report_metrics` | After quality report generation | Volume and quality metrics in `metadata` |
| `config_load_started` / `config_load_succeeded` | Configuration loading phase | — |
| `input_trips_measurement_started` / `input_trips_measurement_succeeded` | Input measurement phase | `finished_trips_read` on `_succeeded` |
| `dim_time_provisioning_started` / `dim_time_provisioning_succeeded` | Provisioning phase | `rows_ensured` on `_succeeded` |
| `trip_facts_creation_started` / `trip_facts_creation_succeeded` | Creation phase | `facts_derived`, `inserted_rows`, `skipped_rows` on `_succeeded` |
| `trip_facts_verification_started` / `trip_facts_verification_succeeded` | Verification phase (read-back) | verification metrics on `_succeeded` |
| `data_quality_validation_started` / `data_quality_validation_succeeded` | Data quality validation phase | verdict of the three checks on `_succeeded` |
| `quality_report_started` / `quality_report_succeeded` | Quality report phase | `quality_report_path` on `_succeeded` |

#### Service events

Emitted by the service before raising an exception (the orchestrator only routes):

| Event | When |
|---|---|
| `input_trips_measurement_failed` | DB error during input measurement |
| `dim_time_provisioning_failed` | DB error during provisioning |
| `trip_facts_creation_failed` | DB error during fact creation |
| `persisted_facts_measurement_failed` | DB error during read-back verification |
| `quality_report_failed` | Error saving the quality report |

### Observability (Loki + Grafana stack)

Observability is based on structured logging: all events are emitted as JSON with the fields `service`, `event`, `status`, `execution_id`, and `correlation_id`. In Airflow, logs are collected by Promtail and forwarded to Loki. All queries follow the pattern:

```
{service="airflow_tasks"} | json | service_extracted="refinedtripfacts" | event="<event>"
```

## Prerequisites

- Tables `refined.trip_facts` and `refined.dim_time` existing in the `sptrans_insights` database (created via `automation/bootstrap_postgres.sh`)
- Table `refined.finished_trips` existing and populated with a non-null `logic_date` (`TIMESTAMPTZ`)
- Dataset `finished_trips_ready` being emitted by `refinedfinishedtrips` with payload `{"logical_date_string": "..."}`
- Metadata bucket in MinIO for storing quality reports
- `.env` file with required credentials (template in `.env.example`)

## Configuration

Configuration is centralized in the `pipeline_configurator` module and exposed as a canonical object with:
- `general`
- `connections`

### Local/dev

- `general` comes from `dags-dev/refinedtripfacts/config/refinedtripfacts_general.json`
- `.env` in `dags-dev/refinedtripfacts/.env` is used only for connection credentials

Expected credentials in `.env`:
```
DB_HOST=<db_hostname>
DB_PORT=<PORT>
DB_DATABASE=<dbname>
DB_USER=<user>
DB_PASSWORD=<password>
DB_SSLMODE="prefer"
MINIO_ENDPOINT=<hostname:port>
ACCESS_KEY=<key>
SECRET_KEY=<secret>
```

Expected keys in `general`:
```json
{
  "tables": {
    "finished_trips_table_name": "refined.finished_trips",
    "trip_facts_table_name": "refined.trip_facts",
    "dim_time_table_name": "refined.dim_time"
  },
  "quality": {
    "completeness_loss_rate_warn_threshold": 0.01,
    "completeness_loss_rate_fail_threshold": 0.05,
    "avg_speed_kmh_max": 120.0
  }
}
```

## Installation instructions

- `cd dags-dev`
- `python3 -m venv .venv`
- `source .venv/bin/activate`
- `pip install -r requirements.txt`

## Database setup required before execution

Before running this pipeline locally, the tables `refined.trip_facts` and `refined.dim_time` must exist in the `sptrans_insights` database.

The recommended operational path is to run the project's PostgreSQL bootstrap:

```bash
./automation/bootstrap_postgres.sh
```

This script applies the SQL files in `database/bootstrap/postgres/`, including `005_refined_trip_facts.sql`.

### Reference schema and partitioning for `refined.trip_facts`

```sql
CREATE TABLE refined.trip_facts (
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
```

Partitioning: daily by `started_at`, 90-day retention via `pg_partman`.

### Reference schema for `refined.dim_time`

```sql
CREATE TABLE refined.dim_time (
    time_key     INTEGER  NOT NULL,
    date_actual  DATE     NOT NULL,
    month        SMALLINT NOT NULL,
    day          SMALLINT NOT NULL,
    hour_of_day  SMALLINT NOT NULL,
    weekday      SMALLINT NOT NULL,
    is_weekend   BOOLEAN  NOT NULL,
    PRIMARY KEY (time_key)
);
```

`time_key` format: `YYYYMMDDHH` in the `America/Sao_Paulo` timezone. Example: `2026060814` = June 8 2026 at 14:00 São Paulo time.

## Airflow (production)

In Airflow, configuration and credentials are managed through Variables and Connections:
- Variable `refinedtripfacts_general` (JSON)
- Credentials via Connections (MinIO and Postgres)

Before executing the DAG in Airflow, the required tables must already exist as described above.

## Tests

```bash
# Unit tests (no database required, default run)
pytest tests/ --ignore=tests/integration

# Integration tests (require a real PostgreSQL, opt-in)
# One-time prerequisite: create the test_sptrans database
bash tests/integration/bootstrap_test_db.sh

pytest tests/integration -m integration
```

Integration test connection is configured in `tests/integration/.env` (use `.env.example` as template). The target database must be `test_sptrans` — never production.

## Data dictionary

**Grain**: one row per finished trip. Primary key: `(started_at, vehicle_id, trip_id)`.

| Column | Type | Source | Derivation rule |
|---|---|---|---|
| `trip_id` | TEXT NOT NULL | `refined.finished_trips.trip_id` | Propagated directly |
| `vehicle_id` | INTEGER NOT NULL | `refined.finished_trips.vehicle_id` | Propagated directly |
| `route_id` | TEXT NOT NULL | `trip_id` | `LEFT(trip_id, LENGTH(trip_id) - 2)` — removes the last two characters that encode direction |
| `direction` | SMALLINT NOT NULL | `trip_id` | `CASE RIGHT(trip_id, 1) WHEN '0' THEN 1 WHEN '1' THEN 2 END` |
| `started_at` | TIMESTAMPTZ NOT NULL | `refined.finished_trips.trip_start_time` | Rename |
| `ended_at` | TIMESTAMPTZ NOT NULL | `refined.finished_trips.trip_end_time` | Rename |
| `duration_seconds` | INTEGER | `refined.finished_trips.duration_seconds` | Propagated directly |
| `duration` | INTERVAL | `duration_seconds` | `make_interval(secs => duration_seconds)` — for dashboard readability |
| `is_circular` | BOOLEAN | `refined.finished_trips.is_circular` | Propagated directly |
| `distance_meters` | DOUBLE PRECISION | `refined.finished_trips.distance_meters` | Propagated directly. Inherited semantics: **linear proxy between terminals for non-circular trips**; **point-to-point distance for circular trips**. The `is_circular` field qualifies the interpretation. |
| `avg_speed_kmh` | DOUBLE PRECISION | `refined.finished_trips.avg_speed_kmh` | Propagated directly |
| `started_at_time_dim_key` | INTEGER NOT NULL | `started_at` | `to_char(trip_start_time AT TIME ZONE 'America/Sao_Paulo', 'YYYYMMDDHH24')::int` |
| `ended_at_time_dim_key` | INTEGER NOT NULL | `ended_at` | `to_char(trip_end_time AT TIME ZONE 'America/Sao_Paulo', 'YYYYMMDDHH24')::int` |
| `logic_date` | TIMESTAMPTZ NOT NULL | `refined.finished_trips.logic_date` | Propagated directly — identifies the ingestion batch that originated the trip |
| `created_at` | TIMESTAMPTZ NOT NULL | system | `DEFAULT NOW()` — insertion timestamp in the refined layer |

**`trip_id` as join key to `refined.trip_details`**: `trip_id` is the consistent join key with `refined.trip_details`. Referential integrity is established upstream in the `transformlivedata` enrichment join (`merge on trip_id`) and is not re-validated by `refinedtripfacts`. The `trip_facts ↔ trip_details` join is a consumption concern (dashboards), and `trip_details` completeness is owned by the GTFS pipeline.

**`started_at_time_dim_key` / `ended_at_time_dim_key`**: reference `refined.dim_time.time_key` **with no `FOREIGN KEY`**. Coverage is guaranteed by load order (provision before creation) and **verified at runtime by the `dim_time_coverage` read-back check** — not by a database constraint. This is deliberate for engine portability: a future Redshift migration would not enforce the FK (Redshift treats FK/PK/UNIQUE as informational), and the read-back quality layer is portable pure SQL aggregates.
