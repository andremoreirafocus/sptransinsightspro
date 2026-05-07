# ADR-0004: Multi-layer data quality framework

**Date:** 2026-04-15  
**Status:** Accepted

## Context

The `transformlivedata` pipeline processes bus-position data extracted from an external API, SPTrans, which is outside the project’s control. Data characteristics:

- **Volume:** tens of thousands of records per execution, representing positions of all buses in circulation every 2 minutes.
- **Unreliable source:** the API may return records with missing fields, out-of-domain values such as invalid coordinates or route IDs not present in GTFS, or a slightly changed structure without warning.
- **Critical downstream use:** transformed data feeds completed-trip analysis in `refinedfinishedtrips` and the real-time visualization layer. Corrupted data in the trusted layer propagates silently to end users.
- **Need for observability:** without visibility into what happens inside the pipeline, it is impossible to distinguish “the API has a problem today” from “I introduced a bug in the transformation” or “the data has always had this quality level.”

The simplest approach, running the transformation and failing the whole execution if any error occurs, would discard entire runs because of a few invalid records, artificially degrading pipeline availability. The opposite approach, silently ignoring errors, would hide real data-quality problems.

## Decision

Implement a **data quality framework with four complementary responsibilities**, executed as sequential stages inside `transformlivedata`:

### 1. Structural validation (pre-transformation)

JSON Schema validates the raw payload received from the API before any transformation. It detects API contract changes, such as removed fields or changed types, before they cause opaque transformation errors. If the payload does not conform to the schema, the pipeline fails immediately with a precise message because there is no useful data to save.

### 2. Transformation with issue collection

During transformation in `transform_positions.py`, semantic problems are **collected, not raised**: records with a non-existent `trip_id`, out-of-domain coordinates, or non-computable distances are marked with the reason for the issue. Transformation continues for the remaining records.

### 3. Quarantine of invalid records

Records that fail validation are isolated in a dedicated `quarantined` bucket in Parquet format, with an additional `quarantine_reason` column. This guarantees:
- Problematic records do not reach the trusted layer.
- Records are not discarded; they can be inspected, investigated, and eventually reprocessed if the problem is fixable.

### 4. Post-transformation statistical validation (Great Expectations)

After transformation, statistical expectations are checked on the resulting DataFrame, such as a minimum rate of records with valid `trip_id` or the absence of nulls in critical columns. Violations are recorded in the quality report, and records that fail expectations are additionally quarantined.

### Quality report

Each execution generates a JSON report with two sections:
- **`summary`**: execution status, `PASS`, `WARN`, or `FAIL`, acceptance rate, and invalid-record counts by category. This is sent via webhook to `alertservice` for notification.
- **`details`**: counts by pipeline stage, transformation metrics, GX expectation summary, and generated artifacts such as Parquet paths and the quarantine report.

### Partial-failure report

If the pipeline fails at any stage after processing has started, for example while saving to trusted, the quality report is generated with the data available up to that point, identifying the failing stage. This enables precise diagnosis instead of relying only on stack traces in logs.

### Column lineage

The `lineage/lineage_functions.py` module automatically tracks the origin of each column in the resulting DataFrame, including source JSON path, applied transformation, and resulting type, adding traceability to the quality report.

## Alternatives considered

**Fail-fast, fail the entire execution at the first error:** Simple to implement. However, a single record with invalid coordinates would bring down processing for tens of thousands of valid records. Perceived pipeline availability would be artificially low.

**Silent best effort, ignore errors and save whatever succeeds:** Maximum availability, but zero observability. Corrupted data reaches the trusted layer and visualization silently. Transformation bugs may take hours or days to be detected, only after users complain about wrong data.

**Structured logging without quarantine:** Log the errors without isolating invalid records. Better than silence, but bad data still reaches the trusted layer, and correlating logs with specific records requires manual information cross-referencing.

**External DQ framework, for example dbt tests or Soda Core:** Mature tools for SQL-oriented data-validation pipelines. However, they introduce heavy external dependencies, require separate connection configuration, and do not integrate naturally with the Python-based Airflow transformation flow. The internal framework integrates natively with the pipeline and with the configuration contract from `pipeline_configurator`.

## Consequences

**Positive:**
- Invalid records do not contaminate the trusted layer, but they are not discarded either; they are preserved in quarantine for investigation and reprocessing.
- `alertservice` receives a structured notification at each execution, enabling proactive monitoring without direct access to Airflow logs.
- Partial failures are diagnosable with precision: the report indicates exactly which pipeline stage failed.
- Historical acceptance rate, stored through `alertservice`, makes it possible to identify gradual degradation in API data quality before it becomes critical.
- Column lineage automatically documents the transformation applied to each field, making audit and debugging easier.

**Negative / Tradeoffs:**
- Significantly higher implementation and maintenance complexity than simpler approaches.
- The quality report is generated and saved at every execution. At high frequency, every 2 minutes, this accumulates many artifacts in the `metadata` bucket and requires a retention policy.
- The webhook to `alertservice` introduces runtime coupling between the pipeline and the notification service: if `alertservice` is unavailable, report delivery fails even though the pipeline itself continues.
- The specific validation tools, JSON Schema and Great Expectations, and the decision to externalize their rules into configuration files are addressed in [ADR-0005](./0005-configuration-driven-validation-EN.md).
