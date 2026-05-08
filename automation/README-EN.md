## Purpose of this subproject

Automate deployment and code-promotion operations, ensuring that lint, SAST, and unit tests run before any production change.

## What this subproject does

- validates code quality (lint with `ruff`, SAST with `bandit`, and unit tests with `pytest`) before any operation
- promotes a pipeline from the development environment (`dags-dev`) to the production environment (`airflow/dags`), also synchronizing the shared modules `infra`, `quality`, and `pipeline_configurator`
- builds and redeploys a microservice through `docker compose`

## Prerequisites

- Python 3.10+
- `ruff`, `bandit`, and `pytest` installed in the Python environment used to run the scripts
- `rsync` installed, for pipeline promotion
- Docker and Docker Compose installed, for microservice deployment
- run the scripts from the `automation/` folder or with the correct path for helper modules

## Available scripts

### `platform_bootstrap_and_start.sh`

Starts the platform with prior database bootstrap to avoid startup failures caused by missing required schemas, tables, and indexes.

**What it does, in order:**
1. Starts `airflow_postgres` and `postgres`
2. Waits until both accept connections
3. Runs `bootstrap_airflow_postgres.sh`
4. Runs `bootstrap_postgres.sh`
5. Starts the rest of the platform with `docker compose up -d`

**Usage:**
```bash
cd automation
./platform_bootstrap_and_start.sh
```

---

### `promote_pipeline.py`

Promotes a pipeline from the development environment to production.

**What it does, in order:**
1. Checks whether the pipeline folder exists in `dags-dev/`
2. Runs lint with `ruff` on the pipeline folder
3. Runs SAST with `bandit` at high severity on the pipeline folder
4. Runs unit tests if the `tests/` folder exists
5. Synchronizes the pipeline folder to `airflow/dags/<pipeline>`, excluding `__pycache__`, `.pytest_cache`, and `tests/`
6. Synchronizes the shared modules `infra`, `quality`, and `pipeline_configurator`

**Usage:**
```bash
cd dags-dev
python3 ../automation/promote_pipeline.py <pipeline_name>
```

**Examples:**
```bash
python3 ../automation/promote_pipeline.py transformlivedata
python3 ../automation/promote_pipeline.py gtfs
python3 ../automation/promote_pipeline.py updatelatestpositions
```

---

### `deploy_service.py`

Builds and redeploys a Docker microservice.

**What it does, in order:**
1. Checks whether the service folder exists
2. Runs lint with `ruff` on the service folder
3. Runs SAST with `bandit` at high severity on the service folder
4. Runs unit tests if the `tests/` folder exists
5. Runs `docker compose build <service>`
6. Runs `docker compose up -d <service>`

**Usage:**
```bash
cd automation
python3 deploy_service.py <docker_compose_service_name> <service_folder>
```

**Examples:**
```bash
python3 deploy_service.py extractloadlivedata extractloadlivedata
python3 deploy_service.py alertservice alertservice
```

---

### `deploy_helpers.py`

Internal helper module. Not executed directly.

Exposes the function `run_code_validations(folder, label, step_offset)`, which runs lint, SAST, and tests in sequence and returns the number of steps consumed. It is used by `promote_pipeline.py` and `deploy_service.py`.

Note: when `<folder>/.venv/bin/python` exists, that interpreter is automatically used for `ruff`, `bandit`, and `pytest`.

---

### `os_command_helper.py`

Internal helper module. Not executed directly.

Exposes the function `run_command(command, error_msg)`, which executes subprocesses and stops execution with an error message if a command fails.

## Typical development flow

```text
dags-dev/<pipeline>  ->  promote_pipeline.py  ->  airflow/dags/<pipeline>
```

1. Develop and test the pipeline in `dags-dev/<pipeline>/`
2. Make sure `pytest <pipeline>/tests/` passes locally
3. Run `promote_pipeline.py <pipeline>` to promote it to production
4. The script validates, synchronizes, and updates shared modules automatically
