## Purpose of this subproject

This subproject extracts GTFS files from the SPTrans developer portal to enrich the data extracted from the API.
The final production implementation runs through the Airflow `gtfs` DAG.
Development happens inside `dags-dev`, which contains the Airflow-based subprojects and speeds up local experimentation.
Configuration is loaded automatically through `pipeline_configurator`, according to the execution environment, either production (Airflow) or local development.

## What this subproject does

- downloads GTFS files from the SPTrans developer portal to enrich bus position data obtained from the API
- validates extracted files before loading them into the raw layer by checking file existence, CSV format, and a minimum number of lines
- saves each file under the `gtfs` "folder" in the raw bucket of the object storage service
- executes the **TRANSFORMATION STAGE** for the GTFS base tables:
  - transforms CSV into Parquet
  - validates with Great Expectations when an expectations suite is configured for the table
  - saves artifacts to staging first in the trusted layer
  - on validation failure: moves staged artifacts to quarantine and produces a consolidated diagnosis
  - on success: moves staged artifacts to the final path for each table
- executes the **ENRICHMENT STAGE** for `trip_details` using a staging-first approach:
  - creates `trip_details` at `trusted/<gtfs_folder>/<staging_subfolder>/trip_details.parquet`
  - validates `trip_details` with Great Expectations when a suite is configured
  - on validation failure: moves the artifact to `trusted/<gtfs_folder>/<quarantined_subfolder>/trip_details.parquet`
  - on success: moves the artifact to `trusted/<gtfs_folder>/trip_details/trip_details.parquet`
- generates a single consolidated quality report per pipeline execution (`EXTRACT & LOAD`, `TRANSFORMATION`, `ENRICHMENT`)
- includes a `trip_details` column-lineage artifact with drift detection (`warning: "lineage drift detected"`) when the output diverges from the declared mapping

## Prerequisites

- obtain credentials by registering on the SPTrans developer portal
- availability of two buckets, one for the raw layer and another for the trusted layer, already created in the object storage service
- creation of an object storage access key registered in the configuration file with read/write access to the raw and trusted layer buckets
- `.env` file with the required credentials
- a template is available in `.env.example`
- creation of the configuration file

## Configuration

Configuration is centralized in the `pipeline_configurator` module and exposed as a canonical object with:
- `general`
- `connections`

### Local/dev

- `general` comes from `dags-dev/gtfs/config/gtfs_general.json`
- `.env` in `dags-dev/gtfs/.env` is used only for connection credentials

Expected credentials in `.env`:
```bash
GTFS_URL="http://www.sptrans.com.br/umbraco/Surface/PerfilDesenvolvedor/BaixarGTFS"
LOGIN=<your login>
PASSWORD=<your password>
MINIO_ENDPOINT=<hostname:port>
ACCESS_KEY=<key>
SECRET_KEY=<secret>
```

Expected keys in `general`:
```json
{
  "extraction": {
    "local_downloads_folder": "gtfs_files"
  },
  "storage": {
    "app_folder": "sptrans",
    "gtfs_folder": "gtfs",
    "raw_bucket": "raw",
    "metadata_bucket": "metadata",
    "quality_report_folder": "quality-reports",
    "quarantined_subfolder": "quarantined",
    "staging_subfolder": "staging",
    "trusted_bucket": "trusted"
  },
  "tables": {
    "trip_details_table_name": "trip_details"
  },
  "data_validations": {
    "expectations_validation": {
      "expectations_suites": [
        "data_expectations_stops",
        "data_expectations_stop_times",
        "data_expectations_trip_details"
      ]
    }
  }
}
```

Expectations artifacts loaded automatically through `pipeline_configurator`:
- `dags-dev/gtfs/config/gtfs_data_expectations_stops.json`
- `dags-dev/gtfs/config/gtfs_data_expectations_stop_times.json`
- `dags-dev/gtfs/config/gtfs_data_expectations_trip_details.json`

### TRANSFORMATION STAGE flow

- Processed tables: `stops`, `stop_times`, `routes`, `trips`, `frequencies`, `calendar`
- Paths in the trusted layer:
  - Staging: `trusted/<gtfs_folder>/<staging_subfolder>/<table>.parquet`
  - Quarantine: `trusted/<gtfs_folder>/<quarantined_subfolder>/<table>.parquet`
  - Final: `trusted/<gtfs_folder>/<table>/<table>.parquet`

### ENRICHMENT STAGE flow (`trip_details`)

- Staging path: `trusted/<gtfs_folder>/<staging_subfolder>/trip_details.parquet`
- Quarantine path: `trusted/<gtfs_folder>/<quarantined_subfolder>/trip_details.parquet`
- Final path: `trusted/<gtfs_folder>/trip_details/trip_details.parquet`
- GX validation for `trip_details` uses the `data_expectations_trip_details` suite when configured.
- On failures after the staging write, the pipeline attempts to quarantine the staged artifact to avoid orphaned residuals.
- The [samples](./samples) folder contains a manually curated example of the `trip_details` artifact: [trip_details.parquet](./samples/trip_details.parquet), for documentation reference only.

### Consolidated quality report

- There is exactly one report per execution of `gtfs-v3.py`, with `summary` + `details`.
- The report consolidates the result of the three phases:
  - `extract_load_files`
  - `transformation`
  - `enrichment`
- Report path:
  - `<metadata_bucket>/<quality_report_folder>/gtfs/year=YYYY/month=MM/day=DD/hour=HH/quality-report-gtfs_<HHMM>_<execution_suffix>.json`
- Report construction and persistence delegate to `quality.reporting` (`build_quality_report_path`, `build_quality_summary`, `save_quality_report`).
- The `summary` section follows the standard contract defined in `quality.reporting`, with GTFS-specific additional fields: `stage`, `validated_items_count`, `relocation_status`, `relocation_error`.
- `acceptance_rate` is a continuous value between 0.0 and 1.0, calculated as `(validated_items_count - rows_failed) / validated_items_count` over the total number of items processed across all phases. It used to be binary (0.0 or 1.0).
- The [samples](./samples) folder contains a manually curated example of the consolidated quality report: [quality-report-gtfs_HHMM_uuid.json](./samples/quality-report-gtfs_HHMM_uuid.json).

### Quality reporting

- On failures in any phase, the pipeline generates and persists a consolidated report with:
  - `failure_phase`
  - `failure_message`
  - per-phase results in `details.stages`
  - `validated_items_count`, `error_details`, `relocation_status`, `relocation_error` for each phase
  - `column_lineage` artifacts in the enrichment stage

### Observability (Loki + Grafana)

The pipeline's observability is based on structured logging: all events are emitted as JSON with the fields `service`, `event`, `status`, `execution_id`. In the Airflow environment, logs are collected by Promtail and sent to Loki.

#### Event taxonomy

Events emitted by the **orchestrator**:

| Event | When | Relevant content |
|---|---|---|
| `execution_started` | Start of execution | `execution_id` |
| `execution_finished` | Execution completed successfully | `execution_id`, `status` |
| `execution_aborted` | Any phase fails and stops the pipeline | `execution_id`, `status`, `metadata.phase` |
| `execution_phase_metrics` | At the end of every execution (success or failure) | `metadata.phase_metrics.<phase>.duration_seconds`, `metadata.overall_status` |

Phases tracked in `execution_phase_metrics`: `extract_load_files`, `transformation`, `enrichment`, `quality_report`.

Events emitted by services — **Extract & Load**:

| Event | When | Relevant content |
|---|---|---|
| `gtfs_extraction_started` | Start of GTFS file download | — |
| `gtfs_extraction_succeeded` | Files extracted successfully | `metadata.file_count`, `metadata.downloads_folder` |
| `gtfs_extraction_failed` | Download failed | `error_type`, `error_message` |
| `raw_validation_started` | Start of raw CSV validation | — |
| `raw_file_validation_error` | A file failed validation | `metadata.file`, `metadata.reason` |
| `raw_validation_completed` | Raw CSV validation finished | — |
| `raw_files_upload_started` | Start of upload to raw storage | — |
| `raw_files_upload_succeeded` | Upload completed | — |

Events emitted by services — **Transformation**:

| Event | When | Relevant content |
|---|---|---|
| `csv_load_started` | Start of loading a CSV from the raw layer | `metadata.table` |
| `csv_load_succeeded` | CSV loaded successfully | `metadata.table` |
| `csv_load_failed` | CSV load failed | `metadata.table` |
| `table_transform_started` | Start of transforming a table | `metadata.table` |
| `table_csv_load_failed` | CSV load failure during transformation | `metadata.table` |
| `table_csv_parse_failed` | CSV parse failure | `metadata.table` |
| `table_validation_started` | Start of GX validation for a table | `metadata.table` |
| `table_validation_failed` | GX validation failed | `metadata.table`, `metadata.failures` |
| `table_validation_skipped` | No GX suite configured for the table | `metadata.table` |
| `table_staging_failed` | Failed to persist to staging in the trusted bucket | `metadata.table` |
| `table_transform_succeeded` | Table transformation completed | `metadata.table` |
| `buffer_save_started` | Start of buffer save to object storage | — |
| `buffer_save_succeeded` | Buffer saved successfully | — |
| `buffer_save_failed` | Buffer save failed | `error_type`, `error_message` |
| `file_relocation_started` | Start of file relocation (staging → final or quarantine) | `metadata.source`, `metadata.destination` |
| `file_relocation_item_failed` | A file failed to relocate | `metadata.file` |
| `file_relocation_completed` | Relocation completed | `metadata.relocated_count` |

Events emitted by services — **Enrichment**:

| Event | When | Relevant content |
|---|---|---|
| `trip_details_creation_started` | Start of `trip_details` table creation | — |
| `trip_details_creation_succeeded` | `trip_details` created and exported to staging | `metadata.row_count`, `metadata.path` |
| `trip_details_creation_failed` | `trip_details` creation failed | `error_type`, `error_message` |

#### Grafana dashboard

The dashboard is at [`observability/grafana/provisioning/dashboards/gtfs.json`](../../../../observability/grafana/provisioning/dashboards/gtfs.json) and is provisioned automatically by Grafana. It uses Loki as the datasource. All queries follow the pattern:

```
{service="airflow_tasks"} | json | service_extracted="gtfs" | event="<event>"
```

![Dashboard gtfs](gtfs_dashboard.png)

Default window: `now-30d`. Refresh: `1h`. The dashboard is organized into three rows:

**Row 1 — Operational health**

| Panel | Type | What it shows | Loki event / field |
|---|---|---|---|
| Executions | Timeseries (dots) | Completed (green) and aborted (red) executions over time | `execution_finished` and `execution_aborted` — `count_over_time [1d]` |
| Completed (last 24h) | Stat (green) | Total successful executions in the last 24h | `execution_finished` — `count_over_time [24h]` |
| Aborted (last 24h) | Stat (red if ≥ 1) | Total aborted executions in the last 24h | `execution_aborted` — `count_over_time [24h]` |
| Phase duration per run (s) | Timeseries | Duration per phase: `extract_load_files`, `transformation`, `enrichment`, `quality_report` | `execution_phase_metrics` — `metadata.phase_metrics.<phase>.duration_seconds` via `avg_over_time [1d]` |

**Row 2 — Data volume**

| Panel | Type | What it shows | Loki event / field |
|---|---|---|---|
| Trip details row count per run | Timeseries | Rows in the `trip_details` table produced per execution | `trip_details_creation_succeeded` — `metadata.row_count` via `last_over_time [1d]` |
| GTFS files extracted per run | Timeseries | Number of GTFS files extracted per execution | `gtfs_extraction_succeeded` — `metadata.file_count` via `last_over_time [1d]` |
| Raw file validation errors per run | Timeseries | Number of raw CSV validation errors | `raw_file_validation_error` — `count_over_time [1d]` |

**Row 3 — Logs**

| Panel | What it shows |
|---|---|
| Recent aborted executions | Filtered stream of `execution_aborted` events with phase and failure message |
| Log stream | All pipeline events in descending order |

### Test rules

- GTFS pipeline tests use fakes under `gtfs/tests/fakes/` and dependency injection.
- Do not use `monkeypatch`: scenarios must be covered with explicit reusable doubles (fakes/stubs).
- To run tests:
  - `pytest gtfs/tests`
  - `pytest gtfs/tests --cov=gtfs --cov-report=term-missing`

### Airflow (production)

In Airflow, configuration and credentials are managed through Variables and Connections stored by Airflow itself, as listed below. Any change to this information must be made through the Airflow UI or through the command line by connecting to the Airflow webserver with `docker exec`.
- Variable `gtfs_general` (JSON)
- Credentials via Connections (GTFS and MinIO)

## Installation instructions

To install the requirements:
- `cd dags-dev`
- `python3 -m venv .venv`
- `source .venv/bin/activate`
- `pip install -r requirements.txt`

## Local execution instructions

Create `dags-dev/gtfs/.env` based on `.env.example` and fill in all fields.

Run:
```bash
python ./gtfs-v<version number>.py
```

Example:
```bash
python ./gtfs-v6.py
```
