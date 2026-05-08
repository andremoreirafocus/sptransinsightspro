## Purpose of this subproject

Synchronize the trip-details table generated in the trusted layer from SPTrans GTFS extraction into the refined layer for use by the visualization layer.
The final production implementation runs through the Airflow `refinedsynctripdetails` DAG.
Development happens inside `dags-dev`, which contains the Airflow-based subprojects and speeds up local experimentation.
Configuration is loaded automatically through `pipeline_configurator`, according to the execution environment, either production (Airflow) or local development.

## What this subproject does

- reads from the trusted layer the trip-details table generated from SPTrans GTFS extraction
- applies a light transformation for the refined layer before persistence, while preserving the trusted table as the canonical reference source
- saves the result into a trip-details table in the refined layer to serve the visualization layer

### Circular-route handling in the refined layer

For circular routes, the refined layer does not blindly replicate the endpoints from the trusted table.

Because the visualization layer consumes origin and destination metadata per operational direction, the pipeline adjusts circular-route records:
- for `-0` trips, it preserves the `first_stop_*` fields
- for `-1` trips, it preserves the `last_stop_*` fields
- on the opposite endpoint, it replaces the name with `Intermediate stop`
- on the opposite endpoint, it sets `id`, `lat`, and `lon` to null

This way:
- the trusted layer keeps canonical metadata for processing
- the refined layer exposes metadata that better matches the semantics expected by the dashboard

## Prerequisites

- availability of the trusted-layer bucket, already created in the object storage service
- availability of the `trip_details` table in the trusted layer
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

- `general` comes from `dags-dev/refinedsynctripdetails/config/refinedsynctripdetails_general.json`
- `.env` in `dags-dev/refinedsynctripdetails/.env` is used only for connection credentials

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
    "gtfs_folder": "gtfs"
  },
  "tables": {
    "trip_details_table_name": "refined.trip_details"
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

Before running this pipeline locally, the `refined.trip_details` table must exist in the `sptrans_insights` database.

The recommended operational path to create the required database artifacts is to run the project's PostgreSQL bootstrap:

```bash
./automation/bootstrap_postgres.sh
```

This script applies the SQL files located in `/database/bootstrap/postgres/`.

### Reference schema for `refined.trip_details`

The command below is kept as documentation reference for the expected table structure:

```sql
CREATE TABLE refined.trip_details (
    id BIGSERIAL PRIMARY KEY,
    trip_id TEXT,
    first_stop_id INTEGER,
    first_stop_name TEXT,
    first_stop_lat DOUBLE PRECISION,
    first_stop_lon DOUBLE PRECISION,
    last_stop_id INTEGER,
    last_stop_name TEXT,
    last_stop_lat DOUBLE PRECISION,
    last_stop_lon DOUBLE PRECISION,
    trip_linear_distance DOUBLE PRECISION,
    is_circular BOOLEAN
);

-- Optimized search index for Power BI
-- This supports searching for a specific route/direction
-- and narrowing it down by bus.
CREATE INDEX idx_trip_lookup
ON refined.trip_details (trip_id);
```

### Airflow (production)

In Airflow, configuration and credentials are managed through Variables and Connections stored by Airflow itself, as listed below. Any change to this information must be made through the Airflow UI or through the command line by connecting to the Airflow webserver with `docker exec`.
- Variable `refinedsynctripdetails_general` (JSON)
- Credentials via Connections (MinIO and Postgres)

Before executing the DAG in Airflow, the `refined.trip_details` table must already exist as described above.

## Local execution instructions

Create `dags-dev/refinedsynctripdetails/.env` based on `.env.example` and fill in all fields.
Make sure the table has been created as described above.

Run:
```bash
python ./refinedsynctripdetails-v<version number>.py
```

Example:
```bash
python ./refinedsynctripdetails-v2.py
```
