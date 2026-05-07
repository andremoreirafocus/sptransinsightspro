## Purpose of this subproject

Check which bus-position files extracted from the SPTrans API have already been published to the raw layer by the `extractloadlivedata` microservice but have not yet been processed by the `transformlivedata` DAG.
The final production implementation runs through the Airflow `orchestratetransform` DAG.
Development happens inside `dags-dev`, which contains the Airflow-based subprojects and speeds up local experimentation.
Configuration is loaded automatically through `pipeline_configurator`, according to the execution environment, either production (Airflow) or local development.

## What this subproject does

- reads from the raw-events table that stores metadata about raw-layer files and identifies which ones have not yet been processed
- starts the transformation DAG for each file that is still pending

## Prerequisites

- availability of the Airflow database, which is used to keep the processed-file table
- `.env` file with the required credentials
- a template is available in `.env.example`
- creation of the configuration file

## Configuration

Configuration is centralized in the `pipeline_configurator` module and exposed as a canonical object with:
- `general`
- `connections`

### Local/dev

- `general` comes from `dags-dev/orchestratetransform/config/orchestratetransform_general.json`
- `.env` in `dags-dev/orchestratetransform/.env` is used only for connection credentials

Expected credentials in `.env`:
```bash
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
  "orchestration": {
    "target_dag": "transformlivedata-v7",
    "wait_time_seconds": 15
  },
  "tables": {
    "raw_events_table_name": "to_be_processed.raw"
  }
}
```

### Airflow (production)

In Airflow, configuration and credentials are managed through Variables and Connections stored by Airflow itself, as listed below. Any change to this information must be made through the Airflow UI or through the command line by connecting to the Airflow webserver with `docker exec`.
- Variable `orchestratetransform_general` (JSON)
- Credentials via Connection (Airflow Postgres)

## Installation instructions

To install the requirements:
- `cd dags-dev`
- `python3 -m venv .venv`
- `source .venv/bin/activate`
- `pip install -r requirements.txt`

## Local execution instructions

Create `dags-dev/orchestratetransform/.env` based on `.env.example` and fill in all fields.

Run:
```bash
python orchestratetransform-v1.py
```

## Structure of the enriched real-time positions table created by this subproject using an equivalent SQL command
