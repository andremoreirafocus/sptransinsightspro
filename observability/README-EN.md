# Observability

This directory contains the configuration for the platform's centralized log observability stack, based on **Grafana + Loki + Promtail + Alertmanager**.

Observability is treated as a cross-platform capability, not only as monitoring for a single service. The project strategy covers three complementary dimensions: log structuring, data-lineage tracking, and execution metrics instrumentation.

The strategy uses structured JSON logs as a standard contract between components. This format makes events machine-readable, simplifies parsing and automated querying, and increases consistency of operational analysis across services and pipelines.

For end-to-end traceability, the project uses `correlation_id` based on `logical_datetime` (timestamp of processed data across pipelines) and `execution_id` (execution correlation). This combination enables cross-stage auditing, faster failure diagnosis, and consistent operational monitoring of flow health.

Pipeline information flow monitored by observability:

`extractloadlivedata` → `transformlivedata` → `refinedfinishedtrips`

As an execution/metrics instrumentation reference, `extractloadlivedata` records per-phase metrics (`extract`, `save`, `notify`) with attempts, successes, failures, and duration, plus the final `execution_metrics_final` event, structured for Prometheus/AlertManager queries and per-execution operational visibility.

## Execution Report Contract

For execution-outcome alerting and monitoring, the final report events are:
- `execution_completed`
- `execution_failed_non_recoverable`

Correlation semantics:
- `execution_id`: primary execution-level correlation key (main key for alerting and execution-outcome monitoring).
- `correlation_id`: request/data-scope correlation key (individual processed item); not required in final execution-outcome events.

Required `metadata` fields for execution report events:
- `execution_seconds`
- `items_total`
- `items_failed`
- `retries_seen`
- `correlation_ids` (list of correlations worked during the execution, ordered and deduplicated)
- `correlation_ids_count` (total number of unique correlations worked)

Meaning of correlation fields in execution report:
- `correlation_ids`: ordered subset (and possibly truncated) of unique correlations processed in the execution, useful for quick diagnosis and operational sampling.
- `correlation_ids_count`: total number of unique correlations processed in the execution, including items not present in `correlation_ids` when truncation occurs.

## Stack

| Component | Role |
|---|---|
| **Loki** | Log aggregation backend. Receives structured log streams and stores them indexed by labels. |
| **Promtail** | Log shipping agent. Scrapes container logs via the Docker socket and forwards them to Loki. |
| **Grafana** | Visualization layer. Queries Loki using LogQL and renders dashboards. |
| **Alertmanager** | Alert management component. Receives rule-based alerts (for example from Loki Ruler) and applies routing, grouping, and deduplication. |

All three services are defined in the root `docker-compose.yml` and share the `rede_fia` network.

## Email Alerting

Loki Ruler evaluates alert rules and sends alerts to Alertmanager, which applies severity-based routing and sends email notifications.

Rules currently configured for `extractloadlivedata`:
- `ServiceFailed` (`severity=critical`): fires on `execution_failed_non_recoverable`.
- `ServiceWarningThreshold` (`severity=warning`): fires when execution completes with `retries_seen > 0`.

Rules currently configured for `transformlivedata`:
- `PipelinePhaseFailed` (`severity=critical`): fires when any pipeline phase emits a failure event.
- `AcceptanceRateBelowThreshold` (`severity=warning`): fires when the record acceptance rate drops below 98%.
- `NoPipelineExecutionCompleted` (`severity=critical`): fires when no `execution_finished` is detected in 30 minutes.

Rules currently configured for `refinedfinishedtrips`:
- `ExecutionAborted` (`severity=critical`): fires on `execution_aborted` — the execution was stopped mid-run (covers freshness and gap failures that reach the fail threshold).
- `NoPipelineExecutionCompleted` (`severity=critical`): fires when no `execution_finished` is detected in 1 hour.
- `PositionFreshnessHigh` (`severity=warning`): fires when `observed_lag_minutes` exceeds the warn threshold of 10 minutes — pipeline continues but data quality may be degraded.
- `ExtractionGapHigh` (`severity=warning`): fires when `max_gap_minutes` exceeds the warn threshold of 5 minutes — early signal before the pipeline starts aborting.

Email configuration used by Alertmanager:
- `ALERTMANAGER_SMTP_HOST`
- `ALERTMANAGER_SMTP_PORT`
- `ALERTMANAGER_SMTP_USER`
- `ALERTMANAGER_SMTP_PASSWORD`
- `ALERTMANAGER_EMAIL_FROM`
- `ALERTMANAGER_EMAIL_TO`

## Architecture

```
extractloadlivedata (stdout JSON logs)
  → Docker runtime
    → Promtail (Docker socket scrape)
      → Loki (label-indexed storage)
        → Grafana (LogQL queries + dashboards)
```

The application emits structured JSON logs to `stdout`. Promtail collects them externally via the Docker socket — the application has no knowledge of the transport layer. This keeps the logging contract stable and the backend swappable per environment.

## Log Contract

Every log line is a JSON object. Mandatory fields:

| Field | Description |
|---|---|
| `timestamp` | UTC timestamp (ISO 8601) |
| `level` | `DEBUG` / `INFO` / `WARNING` / `ERROR` / `CRITICAL` |
| `service` | Service name (e.g. `extractloadlivedata`) |
| `component` | Module or class emitting the log |
| `event` | Stable snake_case event name (e.g. `execution_metrics_final`) |
| `message` | Human-readable description |

Recommended fields: `execution_id`, `correlation_id`, `status`, `metadata`.

## Loki Labels

Promtail indexes the following labels for stream selection in LogQL:

| Label | Value |
|---|---|
| `service` | `extractloadlivedata` |
| `container` | Container name |
| `source` | `docker` |

All other fields (e.g. `event`, `level`, `execution_id`) are parsed at query time using `| json`.

## Dashboards

Dashboards are provisioned automatically from `grafana/provisioning/dashboards/`. No manual import is needed.

| Dashboard | File | Description |
|---|---|---|
| extractloadlivedata | `extractloadlivedata.json` | Executions, errors, warnings, execution time per phase, log stream |
| transformlivedata | `transformlivedata.json` | Executions, duration per phase, acceptance rate, raw record volume, rejected records by reason, log stream |
| refinedfinishedtrips | `refinedfinishedtrips.json` | Executions, duration per phase, trips added, trips detected, positions loaded, extraction quality, position data freshness, log stream |

Dashboard screenshots are in each pipeline's README, alongside the full panel inventory and Loki queries:

- [extractloadlivedata](../extractloadlivedata/README.md#dashboard-grafana) — errors and warnings per execution, execution time per phase
- [transformlivedata](../dags-dev/transformlivedata/README.md#dashboard-grafana) — executions, duration per phase, acceptance rate, record volume
- [refinedfinishedtrips](../dags-dev/refinedfinishedtrips/README.md#dashboard-grafana) — executions, trip volume, extraction quality, position data freshness
- [gtfs](../dags-dev/gtfs/README.md#dashboard-grafana) — executions, duration per phase, trip_details row count, extracted files

After editing a dashboard JSON, bump its `version` field and reload without restarting Grafana:

```bash
curl -X POST http://admin:<password>@localhost:3000/api/admin/provisioning/dashboards/reload
```

## Directory Structure

```
observability/
  loki/
    loki-config.yml          # Loki server config (filesystem storage, single-node)
  alertmanager/
    alertmanager.yml.tmpl    # Alertmanager config template (routing and receivers) — credentials injected at startup from env vars
    Dockerfile               # Custom image with sed-based entrypoint
    entrypoint.sh            # Renders the template at container startup
  promtail/
    promtail-config.yml      # Promtail scrape config (Docker socket, extractloadlivedata filter)
  grafana/
    provisioning/
      datasources/
        loki.yml             # Auto-provisioned Loki datasource
      dashboards/
        dashboards.yml               # Dashboard provider config
        extractloadlivedata.json     # extractloadlivedata dashboard
        transformlivedata.json       # transformlivedata dashboard
        refinedfinishedtrips.json    # refinedfinishedtrips dashboard
```

## Local URLs

| Service | URL |
|---|---|
| Grafana | http://localhost:3000 |
| Alertmanager | http://localhost:9093 |
