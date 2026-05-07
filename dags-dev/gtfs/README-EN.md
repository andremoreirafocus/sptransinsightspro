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
  "notifications": {
    "webhook_url": "disabled"
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

### Quality reporting and notification (`alertservice`)

- On failures in any phase, the pipeline generates and persists a consolidated report with:
  - `failure_phase`
  - `failure_message`
  - per-phase results in `details.stages`
  - `validated_items_count`, `error_details`, `relocation_status`, `relocation_error` for each phase
  - `column_lineage` artifacts in the enrichment stage
- The `summary` section is sent via webhook to the `alertservice` microservice when enabled.
- The summary contains status, failure-phase information, and validation metrics to trigger immediate alerts (`FAIL`) or cumulative alerts (`WARN`) configured in `alertservice`.
- Notification is triggered by the DAG (`_send_webhook_from_report`) after report persistence, separately from the report-building service.

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
python ./gtfs-v3.py
```
