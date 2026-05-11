# ADR-0010: Airflow Datasets for event-driven orchestration

**Date:** 2026-05-04  
**Status:** Proposed

## Context

The project already uses Airflow as the pipeline orchestration layer, but some DAG dependencies were still modeled with independent cron schedules rather than explicit data-publication events.

That design works when the relationship between pipelines is purely temporal. However, for some project flows, the real dependency is semantic: a downstream DAG should start only when the upstream DAG has completed successfully and published a new data artifact that is actually useful for consumption.

The clearest cases are:

- `gtfs` produces `trip_details`, consumed by `refinedsynctripdetails`
- `transformlivedata` produces transformed positions in the trusted layer, consumed by:
  - `updatelatestpositions`
  - `refinedfinishedtrips`

When coordination is done only by cron:

- Downstream DAGs can execute without new upstream data having been published.
- Maintenance depends on manually aligning schedules between related pipelines.
- Dependency modeling stays implicit in time, not explicit in the orchestration graph.

In addition, the project already separates correctly:

- business logic and execution in the services and pipeline layers
- Airflow-specific responsibilities in the versioned wrappers under `dags-dev/` and `airflow/dags/`

That separation must be preserved.

## Decision

Adopt **Airflow Datasets** to represent event-driven dependencies between DAGs whenever the correct relationship is “new upstream data published successfully.”

### Rules of the decision

1. The Dataset must be emitted only in the Airflow orchestration layer.
- Event emission stays in the DAG wrapper, not in service functions or core pipeline logic.

2. Business logic must not know about Airflow Datasets.
- Services and pipelines remain reusable and executable outside Airflow.

3. Event-driven downstream DAGs must consume the Dataset instead of depending on a cron schedule.

4. Backfills must not, by default, emit the same Dataset as the main DAG.
- This avoids unintended fan-out to downstream DAGs during historical reprocessing.

### Covered orchestration chains

- `gtfs` publishes `gtfs://trip_details_ready`
- `refinedsynctripdetails` consumes that Dataset

- `transformlivedata` publishes `sptrans://trusted/transformed_positions_ready`
- `updatelatestpositions` consumes that Dataset
- `refinedfinishedtrips` consumes that Dataset

## Alternatives considered

**Independent cron schedules**

This is the simplest operational solution, but it keeps DAG dependencies implicit in time. That increases maintenance coupling between schedules and allows downstream executions with no new upstream data.

**Explicit DAG-to-DAG trigger, for example `TriggerDagRunOperator` or API calls**

This makes DAG relationships explicit, but introduces stronger coupling to specific orchestrators and DAG names, and the semantics fit less naturally with Airflow’s native dataset model.

**Move event emission into internal pipeline logic**

This was rejected because it mixes orchestration responsibility with execution logic. That would reduce reusability, testability, and portability of the core functions when executed outside Airflow.

## Consequences

**Positive:**

- DAG dependencies become explicit in the orchestration model.
- Downstream DAGs execute in response to successful publication of new upstream data, not merely because of cron coincidence.
- Improved freshness of downstream artifacts.
- Less need for manual maintenance of coupled schedules between related pipelines.
- Architectural separation is preserved: Dataset handling stays in the Airflow wrapper and does not contaminate services or pipelines.

**Negative / Tradeoffs:**

- Event topology becomes more strongly dependent on Airflow-specific resources.
- The dataset graph must be documented clearly to avoid operational opacity.
- Backfill strategies require explicit care to avoid unintentionally triggering downstream pipelines.
- Part of operational behavior is no longer visible through cron alone and requires reading the dataset graph.
