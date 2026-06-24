# ADR-0013: `refinedtripfacts` pipeline and dimensional model for operational analytics

**Date:** 2026-06-16  
**Status:** Accepted

## Context

ADR-0012 established Metabase self-hosted as the BI platform, consuming the refined layer in the analytical PostgreSQL (`postgres`). This ADR decides **what** that layer offers for operational analytics: a dedicated dimensional model, materialized in the database itself.

The central reason is to **decouple the analytical model from the BI tool**. The dimensional model lives in the governed data layer (`refined`), and **not** inside Metabase's internal model/semantic layer. Building the dimensional logic inside Metabase (its data-model layer, computed fields, saved questions) would couple the implementation to the tool: lock-in, an un-versioned and un-governed model trapped inside the tool, and a costly migration if the BI platform ever changes. This is the same anti-coupling principle that motivated ADR-0012 (removing proprietary Power BI lock-in) — we must not replace Power BI lock-in with Metabase lock-in by pushing the model into it. The fact table is a **tool-agnostic** analytical contract, readable by any consumer.

On top of that there is a modeling need: `refined.finished_trips` is the **operational output** of the trip-detection pipeline (`refinedfinishedtrips`) — it carries `trip_id` (but not `route_id`/`direction`), has no time dimension, and is not indexed for BI. The intended operational analytics (frequency by route/hour, P50/P95 duration, reliability, speed, "today vs. same weekday over the past weeks") requires a fact table with precomputed analytical attributes and a time dimension, optimized for slicing.

## Decision

Build the `refinedtripfacts` pipeline, triggered by the `finished_trips_ready` Airflow Dataset, producing **`refined.trip_facts`** (one row per finished trip) and **`refined.dim_time`** (star schema) in the refined PostgreSQL.

- The transformation is **set-based SQL** (PG→PG in the same database: `finished_trips` → `trip_facts`) — not row by row in Python, since source and target share the database — executed by a Python service (`create_trip_facts`), following the same pattern as the other pipelines (services + `pipeline_configurator` + the `infra`/`quality`/`observability` layers + orchestration via Airflow Datasets + structured logging + quality report + lineage governance).
- `trip_facts` **precomputes** `route_id`, `direction`, `duration`, and the SP time keys `*_time_dim_key`; `dim_time` provides temporal grouping (hour/weekday/weekend) without timezone conversion at query time.
- `dim_time` is provisioned **gap-free** from the real span (`MIN(trip_start_time)`/`MAX(trip_end_time)`) of the batch's trips; the load is **idempotent** (`ON CONFLICT DO NOTHING`); completeness is validated; integration tests are isolated in a dedicated test database (`sptrans_insights_test`).
- `trip_facts` is partitioned by day with its own retention (via `pg_partman`), independent of the operational lifecycle of `finished_trips`.

## Alternatives considered

**Build the dimensional model inside Metabase (rejected):** Model the fact/dimension in Metabase's own model layer (computed fields, questions/collections, semantic layer). Rejected because it couples the analytical model to the BI tool (lock-in), keeps the model outside the governed, versioned data layer, and contradicts the anti-coupling principle of ADR-0012. The dimensional model should be a contract in the database, reusable by any consumer.

**Adopt dbt for the transformation (rejected):** Conceptually, the transformation is almost a textbook dbt *incremental model* (`materialized='incremental'`, `unique_key`, a predicate on `logic_date`, a `merge`/`delete+insert` strategy equivalent to `ON CONFLICT DO NOTHING`). dbt would natively provide several mechanisms that are currently hand-rolled:
- **Model contracts** (column names + types) ≈ the schema contract and lineage governance (`validate_trip_facts_lineage`).
- **Relationship test** (every `*_time_dim_key` exists in `dim_time`) ≈ the gap-free coverage test.
- **Schema tests** (not_null, accepted_values) ≈ declarative validations.
- **Targets/profiles**: the centralized, swappable connection the project requires for the integration tests is native (`--target test` vs `--target prod`).
- Lineage DAG and automatic docs.

Why it was rejected nonetheless:
- **Partial coverage of the pipeline.** dbt would cover the transformation *core* + tests + lineage, but **not** the identity of this project's pipelines: structured-event observability (`execution_started/aborted/phase_metrics`, per-activity events, `correlation_id`/`execution_id`, the Loki/Grafana taxonomy), the quality report sent to `alertservice` with WARN/FAIL thresholds, business conditions (`no upstream data → execution_aborted`), and the Airflow Dataset trigger. dbt emits `run_results.json`/`manifest.json`, not that flow — integrating it would require interpreting dbt's artifacts and translating them into the existing taxonomy. A Python *wrapper* for orchestration, observability, and the quality report would therefore remain — i.e., **two paradigms for a single pipeline**.
- **Value below the threshold for a single model.** dbt's value grows with the number of SQL models and shared macros. Here there is essentially **one** model (+ the `dim_time` dimension). Since there will be no new PG→PG pipelines, there is no scale or reuse pressure to justify the cost.
- **Architectural consistency cost.** Introducing dbt for one pipeline breaks the uniformity of the other six (the Python service pattern), and adds a tool, a runtime, CI, and an Airflow integration (cosmos/BashOperator) to serve exactly one transformation.

**No dedicated fact table — query `finished_trips` directly (rejected):** `finished_trips` is the operational output, without `route_id`/`direction` or a time dimension. Dashboards would re-derive attributes and perform timezone conversion on every query, would be coupled to the operational pipeline's internal schema, and would overload a high-write operational table with a second access pattern.

## Consequences

**Positive:**
- A **tool-agnostic** analytical model: the star schema is a contract in the database, governed and versioned, with no lock-in to the BI platform — any consumer (Metabase today, another tomorrow) reads the same `trip_facts`/`dim_time`.
- A BI-ready dimensional model: precomputed attributes + `dim_time` enable fast, temporally correct slicing without timezone conversion at query time.
- Separation of concerns: the analytical serving layer (`trip_facts`/`dim_time`) is independent of the operational output (`finished_trips`) — its own schema, indexes, partitioning, and lifecycle.
- Because `trip_facts` is the durable store of the analytical history, **`finished_trips` does not need long-term retention** — it remains a lean operational handoff, with hourly partitioning and short retention. The trend history lives in the analytical table, not the operational one.
- Architectural consistency: it follows the same pattern as the other pipelines (Python services + structured observability + quality report), with no special case.

**Negative / Tradeoffs:**
- We reimplement by hand what dbt offers out of the box: the schema contract, lineage drift, the gap-free coverage test, and the integration tests. More custom code to maintain.
- The correctness of the SQL semantics depends on an integration-test layer against a real PostgreSQL (test-infrastructure cost) instead of a declarative `dbt test`.
- Partial duplication: a finished trip exists in both `finished_trips` (short term) and `trip_facts` (long term) — a cost accepted in exchange for decoupling operation from analytical serving.

## When to reconsider (the dbt rejection)

The choice not to adopt dbt should be revisited (a new ADR that supersedes it) if:
- The refined layer evolves into a **warehouse with many SQL models** (not just `trip_facts`), raising dbt's value above the threshold.
- The cost of **integrating dbt's artifacts** (`run_results.json`/`manifest.json`) into the existing observability across all pipelines — today disproportionate for a single pipeline — comes to be outweighed by the benefit, removing the dual-paradigm objection.
