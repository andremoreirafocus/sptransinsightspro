# ADR-0008: `alertservice` design (webhook, cumulative alerts, SQLite)

**Date:** 2026-04-15  
**Status:** Accepted

## Context

[ADR-0004](./0004-multi-layer-data-quality-framework-EN.md) establishes that each `transformlivedata` execution generates a quality report with status `PASS`, `WARN`, or `FAIL`, and that this report must trigger notifications for proactive monitoring. The service responsible for receiving these reports and deciding whether and how to notify must solve three independent questions:

1. **How the pipeline delivers the report to the alerting service:** push through webhook or pull through polling?
2. **When a WARN should generate a notification:** immediately or only when accumulated?
3. **Where to persist event history:** which database should be used?

## Decision

### 1. Push through webhook, not polling

`transformlivedata` sends the quality report `summary` through `POST /notify` to `alertservice` at the end of each execution. `alertservice` is an independent FastAPI microservice with no direct access to MinIO or Airflow.

The alternative, having `alertservice` periodically poll reports stored in the MinIO `metadata` bucket, was discarded because it introduces variable latency, since the service would only detect a `FAIL` in the next polling cycle, coupling to MinIO, and state-management logic to track which reports were already processed. With webhooks, notification is processed at the moment the pipeline finishes, with no additional state on the `alertservice` side.

### 2. FastAPI as the framework

`alertservice` exposes a single endpoint, `POST /notify`. FastAPI was chosen because of:
- Automatic input payload validation through Pydantic with `NotificationPayload`, returning HTTP 422 for malformed payloads without custom validation code.
- Startup hook support through `@app.on_event("startup")` for one-time database initialization and pipeline configuration loading, with no reload on every request.
- Low overhead compared to heavier frameworks such as Django or Flask with extensions.

### 3. Cumulative alerts for WARNs

FAILs generate immediate notifications. WARNs, however, are occasionally expected; a single WARN in 24 hours can be normal operational noise. Notifying for every individual WARN would create alert fatigue and lead to systematic disregard of notifications.

The decision was to evaluate WARNs cumulatively inside a time window configurable per pipeline through `warning_window`. An email is sent only if any configured threshold is exceeded within the window:

- `max_failed_rows`: the total number of invalid records accumulated in the window exceeds the absolute limit.
- `max_failed_ratio`: the average failure rate in the window exceeds the configured percentage.
- `max_consecutive_warn`: the number of consecutive WARNs, with no PASS in between, reaches the threshold.

Thresholds and window are defined per pipeline in `config/pipelines.yaml`, allowing independent calibration by context without code changes.

### 4. SQLite for event persistence

Execution history, required for cumulative WARN evaluation, is stored in a local SQLite database, `storage/alerts.db`.

PostgreSQL already exists in the project but was rejected for this purpose for two reasons:
- **Failure isolation:** if PostgreSQL is unavailable, `alertservice` should continue receiving and evaluating notifications normally. Using the same database as the pipelines would make the alerting service unavailable exactly when infrastructure problems are happening, which is when notifications matter most.
- **Operational simplicity:** `alertservice` is a microservice with a low write volume, roughly one row per pipeline execution. SQLite is sufficient and avoids connection pools, schema migrations, and network dependencies.

## Consequences

**Positive:**
- FAIL notifications arrive immediately when pipeline execution ends, with no polling latency.
- Individual WARNs do not create noise; only sustained degradation patterns trigger alerts.
- WARN thresholds are configurable per pipeline without redeployment.
- `alertservice` remains operational even if PostgreSQL is unavailable, which is the correct isolation for a monitoring service.
- SQLite history can be queried locally for debugging without depending on other services.

**Negative / Tradeoffs:**
- The webhook introduces runtime coupling: if `alertservice` is unavailable when `transformlivedata` finishes, the report is not delivered and no notification is triggered, although the pipeline itself continues. There is no automatic retry on the sender side.
- SQLite does not scale to multiple `alertservice` instances because it does not support concurrent writes from distinct processes. If the service were to be horizontally scaled, storage would need migration to PostgreSQL or another centralized database.
- Event history grows indefinitely without a retention policy. Periodic cleanup would need to be added manually.
