# ADR-0002: DuckDB as the transformation engine

**Date:** 2026-04-15  
**Status:** Accepted

## Context

The trusted layer stores data in Parquet format in MinIO (S3-compatible). The transformation pipelines need to execute analytical operations on these files: joins with GTFS tables, time-window filtering, aggregations by vehicle/route/direction, distance calculations, and report generation.

The evaluated options were: processing the data entirely in Pandas by loading DataFrames into memory, adopting a distributed SQL engine such as Presto/Trino or Spark, or using an embedded analytical engine capable of querying Parquet directly in object storage.

The data volume is on the order of tens of thousands of records per execution, from bus positions every 2 minutes, not billions of records.

## Decision

Use **DuckDB** as the transformation engine for all operations that involve reading Parquet from the trusted layer.

DuckDB is an embedded OLAP database, with no server process, capable of:
- Querying Parquet files directly via `httpfs`/`s3` without downloading them locally first.
- Executing full SQL in memory, including joins, window functions, and aggregations.
- Integrating natively with Pandas through `DuckDBPyRelation.df()`.
- Running as a Python library through `import duckdb`, with no external process.

The shared abstraction in `infra/duck_db_v3.py` encapsulates creation of a configured connection with MinIO credentials, exposing a simple interface that can be replaced by fakes in tests.

## Alternatives considered

**Pure Pandas:** Simple and with no extra dependencies. However, it loads entire tables into memory, and expressing joins and aggregations as DataFrame operations is more verbose and less readable than SQL. It would be viable for the current volume, but the code would be significantly harder to maintain.

**Apache Spark:** A horizontally scalable solution for large distributed processing. For this project’s volume, the operational overhead of a cluster, JVM, and Docker Compose deployment would be disproportionate to the benefit. Spark session startup time, around 15 to 30 seconds, would make DAGs that need to run in seconds impractical.

**Presto / Trino:** A highly performant distributed SQL engine for large-scale federated queries. It requires a separate server or cluster, catalog configuration, and process management, which is infrastructure complexity incompatible with the project’s requirements.

**DuckDB (chosen):** Embedded, serverless, native S3 Parquet queries, full SQL, and direct Pandas integration. It does not scale horizontally, but this project’s volumes do not require that.

## Consequences

**Positive:**
- Zero infrastructure overhead: DuckDB runs in the same Python process as the Airflow operator, with no additional containers.
- Reads Parquet via S3 without local materialization: only the required columns and partitions are transferred.
- Expressive SQL for GTFS joins: transformation code is readable as queries instead of long chains of DataFrame operations.
- Testable through `FakeDuckDBConnection`: any function that accepts `duckdb_client` can be tested with a fake and no real infrastructure.

**Negative / Tradeoffs:**
- Single-node: if data volume grows to tens of gigabytes per execution, DuckDB may become a memory bottleneck. Migrating to Spark or Trino would require refactoring both queries and infrastructure.
- No persistence between executions: each DuckDB connection is ephemeral, so state between runs must be explicitly materialized in Parquet.
- DuckDB version affects compatibility of generated Parquet files; version upgrades must be tested carefully.
