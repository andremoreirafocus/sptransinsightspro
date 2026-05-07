## Purpose of this layer

Centralize reusable technical integrations used by the pipelines, while keeping infrastructure code separate from business logic.
This layer provides utility functions for accessing storage systems, databases, and analytical engines.

## What this layer contains

- **Object Storage**: functions for reading, listing, and writing objects (`object_storage.py`)
- **DuckDB**: helper for creating in-memory connections and reading Parquet data (`duck_db_v3.py`)
- **PostgreSQL / SQL**: helpers for executing queries and inserts (`pg_db_v2.py`, `sql_db_v2.py`)

## Notes

- This layer does not contain business rules.
- Pipelines consume these modules by passing the connections already resolved by `pipeline_configurator`.
