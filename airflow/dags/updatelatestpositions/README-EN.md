## Purpose of this subproject

Expose the most recent real-time bus positions in the refined layer for dashboard consumption.
The final production implementation runs through the Airflow `updatelatestpositions` DAG.
Development happens inside `dags-dev`, which contains the Airflow-based subprojects and speeds up local experimentation.
Configuration is loaded automatically through `pipeline_configurator`, according to the execution environment, either production (Airflow) or local development.

## What this subproject does

- reads the most recent real-time positions stored in the `positions` table under the `sptrans` area of the trusted bucket in the object storage service
- saves this data into the refined layer implemented in the low-latency analytical database, for use by the visualization layer

## Prerequisites

- availability of the trusted-layer bucket, already created in the object storage service
- creation of an object storage access key registered in the configuration file with read access to the trusted-layer bucket
- availability of the analytical database service, currently PostgreSQL, for storing data in the refined layer
- `.env` file with the required credentials
- a template is available in `.env.example`
- creation of the configuration file

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
  }
}
```

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
- Variable `updatelatestpositions_general` (JSON)
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
python ./updatelatestpositions-v2.py
```
