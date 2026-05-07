# ADR-0003: Extraction microservice with APScheduler

**Date:** 2026-04-15  
**Status:** Accepted

## Context

Extracting bus positions from the SPTrans API must run at short, regular intervals, initially every 2 minutes, to capture movement with enough granularity for trip analysis. Two requirements are critical:

1. **Interval precision:** deviations of seconds or minutes in extraction frequency create gaps in the position data, producing trips with missing segments or incorrect speed measurements.
2. **Airflow-independent resilience:** if the Airflow scheduler is delayed, for example by full queues or restart, or is unavailable, extraction must not stop.

The Airflow scheduler operates with a minimum resolution of 1 minute through `schedule_interval` and does not guarantee second-level precision: a DAG scheduled every minute can drift by 10 to 30 seconds depending on scheduler load and the number of active DAGs. In addition, extraction availability would become coupled to Airflow availability.

## Decision

Implement extraction as an independent Docker microservice, `extractloadlivedata`, using **APScheduler** with `BlockingScheduler`.

The microservice:
- Runs in its own Docker container, fully independent from Airflow.
- Uses APScheduler with `IntervalTrigger` for configurable interval precision through the `EXTRACTION_INTERVAL_SECONDS` environment variable.
- Persists extracted data locally on disk in `INGEST_BUFFER_PATH` before attempting upload to MinIO, guaranteeing that no data is lost during transient object-storage failures.
- Records processing requests in PostgreSQL, in `to_be_processed.raw`, so Airflow through `orchestratetransform` can identify which files need transformation.
- If PostgreSQL is unavailable, persists the requests on disk through `diskcache` and retries them on the next execution.

## Complementary decision: PostgreSQL as the processing queue

Beyond scheduling, the microservice must tell Airflow which extracted files are ready for transformation. The final decision is the result of three approaches evaluated in sequence.

### Discarded approach 1: DAG scheduled every 2 minutes

The simplest approach would be a `transformlivedata` DAG with `schedule_interval='*/2 * * * *'`: every 2 minutes, the DAG would fetch the most recent file from MinIO and transform it.

The problem is that this approach has no failure memory: if Airflow or MinIO is unavailable during one cycle, that 2-minute window simply is not processed, and no native Airflow mechanism guarantees that the missed execution will later be replayed with the correct file after recovery. The result is silent gaps in the trusted layer that require manual reconciliation to identify and reprocess missed files.

### Discarded approach 2: direct trigger through the Airflow API

To solve reconciliation, the second approach was to make `extractloadlivedata` trigger the `transformlivedata` DAG directly through the Airflow REST API as soon as the file is saved to MinIO, using `NOTIFICATION_ENGINE=airflow`. This guarantees that each saved file generates exactly one transformation trigger.

However, this approach creates fragile bidirectional coupling:

- **Risk of silent loss in both directions:** if the trigger is confirmed by Airflow but the microservice fails immediately afterward, before recording local success, it will try again on the next execution and may create duplicate processing. The inverse is also possible: the microservice records the trigger as successful, but Airflow rejects execution because the DAG-run concurrency limit has been reached.
- **Availability coupling:** if Airflow is unavailable, the microservice retries indefinitely with no certainty that the file will eventually be processed. When Airflow comes back, there is no guarantee that all pending triggers will be replayed in the correct order.
- **Coupling to the DAG name:** the microservice would need the transformation DAG name in configuration, for example `transformlivedata-v8`. A new DAG version would require changing `extractloadlivedata` configuration and restarting the container, coupling release cycles of two otherwise independent services. With the queue, only the `orchestratetransform` DAG, fully managed in the Airflow environment, knows the target DAG name; the ingestion microservice remains completely agnostic to transformation versioning.
- **No ordering guarantee:** multiple concurrent triggers with no queue control may create race conditions in the trusted layer.

### Decision: PostgreSQL table as a durable queue

`extractloadlivedata` records each extracted file as a row in `to_be_processed.raw` with `processed = false`. Airflow, through `orchestratetransform`, polls this table periodically, triggers `transformlivedata` for each pending file in creation order, and marks the row as `processed = true` only after transformation succeeds.

This architecture preserves all PostgreSQL ACID guarantees:

- **Airflow availability:** because PostgreSQL is Airflow’s own backend, if the database is unavailable, Airflow is unavailable too. There is no window where Airflow is available but the database is not. When both return, `orchestratetransform` naturally resumes polling and processes all accumulated pending files, with no manual intervention.
- **`extractloadlivedata` availability:** if the database is unavailable at extraction time, the requests are persisted on disk through `diskcache` and reinserted into the table when the database is back, with no record loss.
- **Exactly-once processing guarantee:** each file leaves the queue only after transformation confirms success. Failures at any stage of `transformlivedata` leave the row as `processed = false`, guaranteeing automatic reprocessing without duplication.
- **Natural ordering:** `orchestratetransform` processes rows in creation order, avoiding race conditions.

Direct trigger through the Airflow API remains supported as an alternative configurable mechanism through `NOTIFICATION_ENGINE=airflow` for development or diagnostic scenarios, but it is not the production path.

---

## Alternatives considered for scheduling

**Airflow DAG with interval sensor:** The simplest approach, a DAG with `schedule_interval='*/2 * * * *'`. However, Airflow’s scheduling precision depends on scheduler load, and extraction availability would become coupled to Airflow availability. A scheduler restart for upgrades or maintenance would interrupt extraction for minutes.

**Operating system cron job:** Reliable precision without Airflow dependency. However, it would require access to the host crontab, outside Docker Compose, making the solution less portable and harder to set up in new environments.

**Kubernetes CronJob:** A robust cloud-production solution with concurrency control and retry policies. However, it introduces dependency on a Kubernetes cluster, which is incompatible with the project’s local Docker Compose execution model.

## Consequences

**Positive:**
- Extraction interval precision independent of Airflow state.
- No extracted file is lost: the PostgreSQL queue guarantees that every pending file will be processed exactly once, even after prolonged unavailability of any service.
- Four-layer resilience: API with retry and backoff, MinIO with a local disk buffer, PostgreSQL with a disk-backed cache via `diskcache`, and a durable queue in `to_be_processed.raw`.
- Configurable interval without redeployment through `EXTRACTION_INTERVAL_SECONDS`.
- Independent deploy and rollback: the microservice can be updated without touching Airflow.

**Negative / Tradeoffs:**
- An additional service to monitor, deploy, and maintain, including Dockerfile, environment variables, and disk volume.
- Monitoring is split: extraction failures appear in container logs, not in the Airflow UI.
- The `to_be_processed.raw` table becomes a central coordination point. Corruption or uncontrolled growth, for example a persistent `transformlivedata` failure without cleanup, can accumulate rows indefinitely and requires backlog monitoring.
