# Architectural Decision Records (ADRs)

An ADR documents a significant architectural decision: the context that motivated it, what was decided, the alternatives that were rejected, and the resulting consequences, both positive and negative.

The goal is not to justify choices after the fact, but to record the reasoning at the moment of the decision so future collaborators understand *why* the code is the way it is, not just *what* it does.

## How to use

- Each ADR is immutable after it is accepted. If a decision is revised, a new ADR supersedes the previous one and the older ADR should be marked as `Superseded by ADR-XXXX`.
- To propose a new decision, create a file following the pattern `NNNN-title-in-kebab-case.md` with status `Proposed`, following the structure of the existing ADRs.
- Number sequentially from the next available identifier.

---

## Decision index

| ADR | Title | Status |
|-----|-------|--------|
| [ADR-0001](./0001-medallion-architecture-EN.md) | Medallion Architecture (Raw -> Trusted -> Refined) | Accepted |
| [ADR-0002](./0002-duckdb-as-transformation-engine-EN.md) | DuckDB as the transformation engine | Accepted |
| [ADR-0003](./0003-extraction-microservice-with-apscheduler-EN.md) | Extraction microservice with APScheduler | Accepted |
| [ADR-0004](./0004-multi-layer-data-quality-framework-EN.md) | Multi-layer data quality framework | Accepted |
| [ADR-0005](./0005-configuration-driven-validation-EN.md) | Configuration-driven validation (JSON Schema + Great Expectations) | Accepted |
| [ADR-0006](./0006-dependency-injection-as-testability-strategy-EN.md) | Dependency injection as a testability strategy | Accepted |
| [ADR-0007](./0007-pipeline-promotion-workflow-EN.md) | Pipeline promotion workflow (`dags-dev` -> `airflow/dags`) | Accepted |
| [ADR-0008](./0008-alertservice-design-EN.md) | `alertservice` design (webhook, cumulative alerts, SQLite) | Accepted |
| [ADR-0009](./0009-airflow-datasets-for-event-driven-orchestration-EN.md) | Airflow Datasets for event-driven orchestration | Proposed |
