## Purpose of this subproject

Centralize pipeline configuration loading and validation, ensuring consistency between the local development environment and production (Airflow).
`pipeline_configurator` provides a canonical configuration contract, schema validation through Pydantic, and standardized credential loading.

## What this subproject does

- defines a single entry point for loading pipeline configurations
- validates the contents of `general` using pipeline-specific Pydantic schemas
- normalizes the output to the canonical contract:
  - `general`
  - `connections`
  - validation artifacts declared in `data_validations` inside `general` (optional): `raw_data_json_schema` and `data_expectations`
- automatically resolves the execution environment (local vs Airflow)
- avoids duplication of configuration logic inside pipelines

## Prerequisites

- JSON configuration files per pipeline (`{pipeline}_general.json`)
- pipeline-specific Pydantic schemas (`{pipeline}_config_schema.py`)
- credentials configured in `.env` (local/dev) or in Airflow (Connections/Variables)

## Configuration

The module is exposed in `pipeline_configurator/config.py` through the `get_config` function.

### Local/dev

- `general` comes from `dags-dev/{pipeline}/config/{pipeline}_general.json`
- validation artifacts listed in `data_validations` come from `dags-dev/{pipeline}/config/{pipeline}_{name}.json`
- credentials are read from `.env` in `dags-dev/{pipeline}/.env`

### Airflow (production)

- `general` comes from the Variable `{pipeline}_general`
- validation artifacts listed in `data_validations` come from the Variable `{pipeline}_{name}`
- credentials are read through Airflow Connections (for example MinIO, Postgres, HTTP)

## Canonical contract

Example output for a pipeline with `data_validations` declared:
```json
{
  "general": { ... },
  "connections": {
    "object_storage": { ... },
    "database": { ... },
    "http": { ... }
  },
  "raw_data_json_schema": { ... },
  "data_expectations": { ... }
}
```

Validation keys (`raw_data_json_schema`, `data_expectations`, and so on) are loaded automatically from the `data_validations` section in `general`. Pipelines without that section receive only `general` and `connections`.

## Usage example

```python
from pipeline_configurator.config import get_config
from mypipeline.config.mypipeline_config_schema import GeneralConfig

PIPELINE_NAME = "mypipeline"

pipeline_config = get_config(
    PIPELINE_NAME,
    None,  # env override (None = auto)
    GeneralConfig,
    http_conn_name=None,
    object_storage_conn_name="minio_conn",
    database_conn_name="postgres_conn",
)
```
