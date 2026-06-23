## Purpose of this subproject

Automate deployment and code-promotion operations, ensuring that lint, SAST, type checking, and unit tests run before any production change.

## What this subproject does

- validates code quality (lint with `ruff`, SAST with `bandit`, and unit tests with `pytest`) before any operation
- validates static typing with `mypy` before any operation
- promotes a pipeline from the development environment (`dags-dev`) to the production environment (`airflow/dags`), also synchronizing the shared modules `infra`, `quality`, `observability`, and `pipeline_configurator`
- builds and redeploys a microservice through `docker compose`

## Prerequisites

- Python 3.10+
- `ruff`, `bandit`, `mypy`, and `pytest` installed in the Python environment used to run the scripts
- `rsync` installed, for pipeline promotion
- Docker and Docker Compose installed, for microservice deployment
- run the scripts from the `automation/` folder or with the correct path for helper modules

## Available scripts

### `platform_bootstrap_and_start.sh`

Starts the platform with prior infrastructure and Airflow bootstrap to avoid startup failures caused by missing required artifacts.

**What it does, in order:**
1. Starts `airflow_postgres`, `postgres`, and `minio`
2. Waits until the infrastructure services become available
3. Runs `bootstrap_minio.sh`
4. Runs `bootstrap_airflow_postgres.sh`
5. Runs `bootstrap_postgres.sh`
6. Starts `airflow_webserver` and `airflow_scheduler`
7. Runs `bootstrap_airflow_app.sh`
8. Runs `bootstrap_observability.sh`
9. Runs `bootstrap_extractloadlivedata.sh`
10. Starts the rest of the platform with `docker compose up -d`
11. Runs `bootstrap_metabase.sh`
12. Runs `bootstrap_metabase_dashboard.sh`

**Usage:**
```bash
cd automation
./platform_bootstrap_and_start.sh
```

---

### `bootstrap_minio.sh`

Ensures that the platform access credential and the required buckets exist in MinIO.

**What it does, in order:**
1. Waits until MinIO becomes available
2. Authenticates with `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD`
3. Ensures the access user defined by `MINIO_PLATFORM_ACCESS_KEY` and `MINIO_PLATFORM_SECRET_KEY` exists
4. Attaches the `readwrite` policy when that user is created for the first time
5. Provisions the buckets declared in `minio_buckets.json` (idempotent)

**Usage:**
```bash
cd automation
./bootstrap_minio.sh
```

---

### `minio_buckets.json`

Declares the buckets that must exist in MinIO. Read by `bootstrap_minio.sh` during bootstrap.

To add a new bucket, add an entry to this file — no script changes are needed.

```json
{
  "buckets": [
    { "name": "raw" },
    { "name": "trusted" },
    { "name": "quarantined" },
    { "name": "metadata" }
  ]
}
```

---

### `bootstrap_airflow_app.sh`

Ensures bootstrap for the Airflow application layer.

**What it does, in order:**
1. Waits until the Airflow CLI is usable in `airflow_webserver`
2. Ensures the admin user defined in `.env` exists
3. Imports the bootstrap Airflow Variables
4. Imports the bootstrap Airflow Connections

**Usage:**
```bash
cd automation
./bootstrap_airflow_app.sh
```

---

### `bootstrap_observability.sh`

Ensures bootstrap for the observability stack.

**What it does, in order:**
1. Starts `loki`, `promtail`, `grafana`, and `alertmanager`
2. Waits for `loki`, `grafana`, and `alertmanager` through HTTP readiness/health endpoints
3. Waits for `promtail` through the log signal `server listening on addresses`

**Usage:**
```bash
cd automation
./bootstrap_observability.sh
```

---

### `bootstrap_extractloadlivedata.sh`

Ensures the `extractloadlivedata` service is built and started, with automatic fallback when BuildKit fails.

**What it does, in order:**
1. Tries `docker compose build extractloadlivedata`
2. If the BuildKit build fails, retries with `DOCKER_BUILDKIT=0`
3. Starts the service with `docker compose up -d extractloadlivedata`

**Usage:**
```bash
cd automation
./bootstrap_extractloadlivedata.sh
```

---

### `bootstrap_metabase.sh`

Ensures both the infrastructure bootstrap **and** the idempotent provisioning of the Metabase application, so the platform comes up from scratch with no manual UI step.

**What it does, in order:**
1. Waits until `postgres` becomes available
2. Runs the SQL bootstrap (`004_metabase.sql`): internal DB, internal user, read-only user, and `SELECT` grants on the `refined` schema
3. Starts the `metabase` service and waits for `GET /api/health`
4. **Provisions the Metabase application** (idempotent, via `curl` + `jq` against `http://localhost:3001`):
   - creates the admin user and completes the wizard (`POST /api/setup`) if not yet set up
   - sets the query timezone in two layers: a reader-role-scoped session default (`ALTER ROLE … IN DATABASE … SET timezone = 'America/Sao_Paulo'`, authoritative for native SQL) and the Metabase **Report Timezone**
   - ensures the read-only `sptrans_insights` datasource scoped to the `refined` schema (`host=postgres`)
   - triggers a schema sync
   - aborts with a non-zero exit on any non-2xx HTTP response; passwords are never logged

**Required environment variables** (besides the Metabase DB/user ones): `METABASE_ADMIN_EMAIL`, `METABASE_ADMIN_PASSWORD` (must satisfy Metabase's password policy). Optional with defaults: `METABASE_ADMIN_FIRST_NAME` (`Admin`), `METABASE_ADMIN_LAST_NAME` (`User`), `METABASE_SITE_NAME` (`SPTrans Insights Pro`). Requires `curl` and `jq` on the host.

**Usage:**
```bash
cd automation
./bootstrap_metabase.sh
```

---

### `bootstrap_metabase_dashboard.sh`

Idempotently provisions the `SPTrans Insights` dashboard in Metabase, creating the collection, native questions (cards), and the layout with global filters. On a re-run, deletes the existing dashboard before recreating — allowing the script to be run at any time to reapply the configuration.

**What it does, in order:**
1. Authenticates as admin and retrieves the `sptrans_insights` datasource ID
2. Creates (or reuses) the `SPTrans Insights Pro` collection
3. Resolves the field IDs required for field filters
4. Creates 14 native cards from the SQL files in `metabase/dashboard_queries/`
5. Creates the dashboard, defines the 5 global filters, and places the dashcards
6. Sets the latitude and longitude semantic types for the live fleet position map
7. Prints instructions for manually configuring auto-refresh in the UI

**Required environment variables:** `METABASE_ADMIN_EMAIL`, `METABASE_ADMIN_PASSWORD`. Optional with defaults: `METABASE_PORT` (`3001`). Requires `curl` and `jq` on the host.

**Usage:**
```bash
cd automation
./bootstrap_metabase_dashboard.sh
```

---

### `promote_pipeline.py`

Promotes a pipeline from the development environment to production. One of the flags `--check` or `--prod` is required.

**Flags**
- `--check`: runs validations only (lint, SAST, tests, type-check). No sync.
- `--prod`: runs validations and syncs to production.

**What it does, in order (both flags):**
1. Checks whether the pipeline folder exists in `dags-dev/`
2. Runs lint with `ruff` on the pipeline folder
3. Runs SAST with `bandit` at high severity on the pipeline folder
4. Runs unit tests if the `tests/` folder exists
5. Runs type checking with `mypy` on the pipeline folder

**With `--prod` only:**
6. Synchronizes the pipeline folder to `airflow/dags/<pipeline>`, excluding `__pycache__`, `.pytest_cache`, and `tests/`
7. Synchronizes the shared modules `infra`, `quality`, `observability`, and `pipeline_configurator`

**Usage:**
```bash
cd dags-dev
python3 ../automation/promote_pipeline.py <pipeline_name> --check
python3 ../automation/promote_pipeline.py <pipeline_name> --prod
```

**Examples:**
```bash
# Validate only
python3 ../automation/promote_pipeline.py transformlivedata --check
python3 ../automation/promote_pipeline.py gtfs --check

# Validate and promote to production
python3 ../automation/promote_pipeline.py transformlivedata --prod
python3 ../automation/promote_pipeline.py gtfs --prod
```

---

### `deploy_service.py`

Builds and redeploys a Docker microservice. One of the flags `--check` or `--prod` is required.

**Flags**
- `--check`: runs validations only (lint, SAST, tests, type-check). No build or deploy.
- `--prod`: runs validations, build, and deploy.

**What it does, in order (both flags):**
1. Checks whether the service folder exists
2. Runs lint with `ruff` on the service folder
3. Runs SAST with `bandit` at high severity on the service folder
4. Runs unit tests if the `tests/` folder exists
5. Runs type checking with `mypy` on the service folder

**With `--prod` only:**
6. Runs `docker compose build <service>`
7. Runs `docker compose up -d <service>`

**Usage:**
```bash
cd automation
python3 deploy_service.py <docker_compose_service_name> <service_folder> --check
python3 deploy_service.py <docker_compose_service_name> <service_folder> --prod
```

**Examples:**
```bash
# Validate only
python3 deploy_service.py extractloadlivedata extractloadlivedata --check

# Validate and deploy
python3 deploy_service.py extractloadlivedata extractloadlivedata --prod
```

---

### `deploy_helpers.py`

Internal helper module. Not executed directly.

Exposes the function `run_code_validations(folder, label, step_offset, total_steps)`, which runs lint, SAST, and tests in sequence and returns the number of steps consumed. It is used by `promote_pipeline.py` and `deploy_service.py`.

Note: when `<folder>/.venv/bin/python` exists, that interpreter is automatically used for `ruff`, `bandit`, `mypy`, and `pytest`.

---

### `os_command_helper.py`

Internal helper module. Not executed directly.

Exposes the function `run_command(command, error_msg)`, which executes subprocesses and stops execution with an error message if a command fails.

---

### `wait_helpers.sh`

Internal helper module for DRY service-wait logic.

Exposes functions reused by bootstrap scripts:
- `wait_for_condition(label, timeout_seconds, interval_seconds, cmd...)`
- `check_http_url(url)`

## Typical development flow

```text
dags-dev/<pipeline>  ->  promote_pipeline.py  ->  airflow/dags/<pipeline>
```

1. Develop and test the pipeline in `dags-dev/<pipeline>/`
2. Make sure `pytest <pipeline>/tests/` passes locally
3. Run `promote_pipeline.py <pipeline> --check` to validate
4. Run `promote_pipeline.py <pipeline> --prod` to promote to production
