## Purpose of this subproject

Expose the most recent real-time bus positions in the refined layer for dashboard consumption.
The final production implementation runs through the Airflow `updatelatestpositions` DAG.
Development happens inside `dags-dev`, which contains the Airflow-based subprojects and speeds up local experimentation.
Configuration is loaded automatically through `pipeline_configurator`, according to the execution environment, either production (Airflow) or local development.

## What this subproject does

- Reads the most recent real-time positions stored in the trusted layer of the object storage service
- Evaluates the freshness of the data read relative to the current time, emitting observability events with the observed lag
- Saves the data into the refined layer implemented in the low-latency analytical database, for use by the visualization layer

### Freshness evaluation

After reading the positions, the pipeline evaluates the time elapsed since the most recent vehicle timestamp to the current time (São Paulo timezone).

- If the observed lag exceeds the warning threshold (`freshness_warn_staleness_minutes`), an observability event is emitted at warning level
- If the lag exceeds the failure threshold (`freshness_fail_staleness_minutes`), the Loki alert is triggered
- The emitted event is `freshness_evaluation` and carries `observed_lag_minutes`, `warn_threshold_minutes`, and `fail_threshold_minutes`
- Thresholds are configurable via `general.quality` in the configuration file

## Prerequisites

- Availability of the trusted-layer bucket, already created in the object storage service
- Creation of an object storage access key registered in the configuration file with read access to the trusted-layer bucket
- Availability of the analytical database service, currently PostgreSQL, for storing data in the refined layer
- `.env` file with the required credentials
- A template is available in `.env.example`
- Creation of the configuration file

## Configuration

Configuration is centralized in the `pipeline_configurator` module and exposed as a canonical object with:
- `general`
- `connections`

### Local/dev

- `general` comes from `dags-dev/updatelatestpositions/config/updatelatestpositions_general.json`
- `.env` in `dags-dev/updatelatestpositions/.env` is used only for connection credentials

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
  "storage": {
    "trusted_bucket": "trusted",
    "app_folder": "sptrans"
  },
  "tables": {
    "positions_table_name": "positions",
    "latest_positions_table_name": "refined.latest_positions"
  },
  "quality": {
    "freshness_warn_staleness_minutes": 10,
    "freshness_fail_staleness_minutes": 30
  }
}
```

### Execution phase metrics

- The DAG emits a structured `execution_phase_metrics` event at the end of each run.
- The event includes `execution_id`, `overall_status`, `total_duration_seconds`, and `phase_metrics`.
- Tracked phases:
  - `config_load`
  - `update_latest_positions`
- On failures, the event is emitted with `overall_status="failed"` before the final exception is raised.

### Event taxonomy

All events follow the structured logging standard and are queryable in Loki via `event="<name>"`.

**Orchestrator events:**

| Event | Description |
|---|---|
| `execution_started` | DAG execution started |
| `config_load_started` | Configuration loading started |
| `config_load_succeeded` | Configuration loaded successfully |
| `execution_phase_metrics` | Phase duration metrics emitted at end of execution |
| `execution_finished` | Execution completed successfully |
| `execution_aborted` | Execution aborted due to failure |

**Service events:**

| Event | Description |
|---|---|
| `path_discovery_started` | Search for the most recent parquet file started |
| `path_discovery_succeeded` | Path found successfully |
| `path_discovery_empty` | No file found within the 2-hour window |
| `path_discovery_failed` | Path discovery failed |
| `prefix_scan_started` | Prefix scan in object storage started |
| `positions_update_skipped` | Update skipped due to absence of recent data |
| `positions_query_started` | DuckDB query against parquet started |
| `positions_query_succeeded` | Query completed successfully |
| `freshness_evaluation` | Freshness evaluation of the data read |
| `positions_save_started` | Persistence to the refined layer started |
| `positions_save_succeeded` | Data persisted successfully |
| `positions_update_failed` | Position update failed |

### Loki alerts

Alert rules are defined in `observability/loki/rules/fake/updatelatestpositions-alerts.yaml`.

| Alert | Severity | Condition |
|---|---|---|
| `ExecutionAborted` | critical | Any execution aborted in the last 5 min |
| `NoPipelineExecutionCompleted` | critical | No `execution_finished` in the last 10 min |
| `PositionFreshnessHigh` | warning | `observed_lag_minutes` above 10 min in the last 10 min |

### Grafana dashboard

The dashboard is available at `observability/grafana/provisioning/dashboards/updatelatestpositions.json` and is provisioned automatically by Grafana.

![Dashboard updatelatestpositions](updatelatestpositions_dashboard.png)

## Installation instructions

To install the requirements:
- `cd dags-dev`
- `python3 -m venv .venv`
- `source .venv/bin/activate`
- `pip install -r requirements.txt`

## Database setup required before execution

Before running this pipeline locally, the `refined.latest_positions` table must exist in the `sptrans_insights` database.

The recommended operational path to create the required database artifacts is to run the project's PostgreSQL bootstrap:

```bash
./automation/bootstrap_postgres.sh
```

This script applies the SQL files located in `/database/bootstrap/postgres/`.

### Reference schema for `refined.latest_positions`

The command below is kept as documentation reference for the expected table structure:

```sql
CREATE TABLE refined.latest_positions (
    id BIGSERIAL PRIMARY KEY,
    veiculo_ts TIMESTAMPTZ,        -- ta: UTC timestamp
    veiculo_id INTEGER,            -- p: vehicle id
    veiculo_lat DOUBLE PRECISION,  -- py: latitude
    veiculo_long DOUBLE PRECISION, -- px: longitude
    linha_lt TEXT,                 -- c: full route sign
    linha_sentido INTEGER,         -- sl: direction
    trip_id TEXT
);
```

### Airflow (production)

In Airflow, configuration and credentials are managed through Variables and Connections stored by Airflow itself, as listed below. Any change to this information must be made through the Airflow UI or through the command line by connecting to the Airflow webserver with `docker exec`.
- Variable `updatelatestpositions_general` (JSON) — imported from `airflow/variables_and_connections/updatelatestpositions_general.json`
- Credentials via Connections (MinIO and Postgres)

Before executing the DAG in Airflow, the `refined.latest_positions` table must already exist as described above.

## Local execution instructions

Create `dags-dev/updatelatestpositions/.env` based on `.env.example` and fill in all fields.
With the table already created as described above, run:

```bash
python ./updatelatestpositions-v<version number>.py
```

Example:
```bash
python ./updatelatestpositions-v4.py
```
