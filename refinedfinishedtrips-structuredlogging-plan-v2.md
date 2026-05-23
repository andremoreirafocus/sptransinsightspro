# refinedfinishedtrips — Structured Logging Migration Plan v2

**Guiding rule:** one concern per step. All existing tests must pass after every step.

---

## Design principles

These principles were established while fixing `transformlivedata` and MUST be applied consistently.

### 1 — Auto-derive service/component from `__name__`
Services always call `get_structured_logger(logger_name=__name__)` — never hardcode `service` or `component`.
The orchestrator is the only place that may override: `get_structured_logger(service=pipeline_name, component="orchestrator", logger_name=__name__)`.

### 2 — Orchestrator owns phase lifecycle events
Orchestrator emits: `<phase>_started`, `<phase>_succeeded`, and only business-condition `<phase>_failed` (when it detects a bad result without an exception, e.g. empty DataFrame).

### 3 — Services own exception-originated failures
When a service raises an exception it MUST log the failure first (component auto-derived from `__name__`), then raise. The orchestrator catches and **only routes** — no additional failure log.
Routing means: `tracker.finish("phase", "failed")` → `handle_failure_event(...)` → `handle_phase_metrics_event(...)` → `create_execution_aborted_log_record(msg, phase="<phase>")` → re-raise.

### 4 — One event per real moment — no duplicates
If the orchestrator logs `<phase>_started` and a service also logs a start for the same operation, one must be removed. Service sub-operations with unique context (e.g. SQL query details, file paths) are NOT duplicates — they are finer-grained events.

### 5 — Exception messages must include key context
When removing a service failure event, the `raise ValueError(f"... '{key_value}': {e}")` must embed the metadata that was in the event (e.g. s3_path, bucket, table name) so `error_message` at the orchestrator is self-contained.

### 6 — Unified events with `target` field
When the same operation runs against different targets, use one event name with a `target` metadata field rather than separate event names:
`save_positions_started` + `{"target": "trusted"}` instead of `save_trusted_started`.

### 7 — Event names encode source
The event name already identifies the component (`get_recent_positions_failed` → clearly from `get_recent_positions`). The `component` field from `__name__` reinforces this but is not the primary mechanism.

### 8 — `execution_aborted` carries `phase` in metadata
`create_execution_aborted_log_record(error_msg, phase="<phase_name>")` — every failure path emits `execution_aborted` with `metadata={"phase": "..."}` and message prefix `"Pipeline aborted: ..."` so failures are queryable by phase in Loki without string parsing.

### 9 — `_OrchestratorEvent` vs `_ServiceEvent` split
`_OrchestratorEvent`: phase lifecycle (started/succeeded), business-condition failures, routing events (`execution_started`, `execution_finished`, `execution_phase_metrics`, `execution_aborted`, `failure_report_*`).
`_ServiceEvent`: all exception-originated failures, sub-operation started/succeeded, metrics/artifact events.
`EventType = Union[_OrchestratorEvent, _ServiceEvent]` — both layers must use the domain logger with `EventType` narrowing.

---

## Context propagation (implemented in observability module)

`execution_id` and `correlation_id` are set once at the start of each pipeline run via `set_execution_context(execution_id, correlation_id)` from `observability.structured_event_logger`. `StructuredEventLogger.emit()` reads them from the context var automatically — no explicit kwargs needed on individual calls. Services are stateless and do not call `set_execution_context`; their log events carry no execution context by design.

---

## Behavioral contract (must hold after every step)

| Test | Contract |
|---|---|
| `test_positions_fail_raises_and_save_not_called` | `ValueError` raised; save not called |
| `test_positions_fail_failure_report_called` | failure report called with `failure_phase="positions_quality"` |
| `test_positions_fail_final_report_not_called` | final report never called on FAIL |
| `test_positions_warn_no_early_report_called` | WARN does not call any report early |
| `test_positions_warn_pipeline_continues_and_save_called` | WARN does not stop pipeline; save called |
| `test_trip_extraction_failure_calls_create_failure_report_with_positions_result` | failure report called with `positions_result`, `trips_result=None`, `persistence_result=None` |
| `test_persistence_failure_calls_create_failure_report_with_partial_results` | failure report called with `positions_result`, `trips_result`, `persistence_result=None`, `column_lineage` |
| `test_all_phases_pass_final_report_called` | final report called with all three results and `column_lineage` |
| `test_all_phases_pass_final_report_status_pass` | final report summary status is `"PASS"` |
| `test_trip_extraction_metrics_reach_final_report` | `extraction_metrics` reaches `validate_trips_quality` and the final report |

---

## Step 1 — REPLACEMENT: old logger → structured logger

**Zero flow changes. Only replaces log calls.**

---

### `extract_trips.py` (orchestrator — uses `RefinedFinishedTripsLogger`)

**1.** Remove `import logging` and `logger = logging.getLogger(__name__)`.

**2.** Reorder the top of the function body so `structured_logger` and `state` come before the first log call, and call `set_execution_context` immediately after `state` is created:

```python
execution_id = str(uuid.uuid4())
run_ts = datetime.now(timezone.utc)
structured_logger = RefinedFinishedTripsLogger(
    get_structured_logger(service="refinedfinishedtrips", component="orchestrator", logger_name=__name__)
)
state = PipelineTaskRunState(execution_id=execution_id, correlation_id=execution_id, run_ts=run_ts)
set_execution_context(state.execution_id, state.correlation_id)
tracker = ExecutionPhaseMetricsTracker(...)
```

Add `set_execution_context` to the import from `observability.structured_event_logger`. Remove the `# upgraded in Step 5` comment.

**3.** Replace each log call. No `execution_id` or `correlation_id` kwargs — context propagation handles them:

| Old call | Event | Level | Extra kwargs |
|---|---|---|---|
| `"Starting execution"` | `execution_started` | info | `status="STARTED"` |
| `"Configuration load succeeded"` | `config_load_succeeded` | info | `status="SUCCEEDED"` |
| `logger.error(error_msg)` in config_load except | — | — | Remove — orchestrator only routes (Principle 3) |
| `f"Starting pipeline run..."` | `positions_load_started` | info | `status="STARTED"` |
| `f"Validating quality of {len(...)} position records."` | `positions_quality_started` | info | `status="STARTED"`, `metadata={"record_count": len(df_recent_positions)}` |
| `f"Trip extraction failed: {failure_message}"` | — | — | Remove — service logs it (Principle 3) |
| `f"Persistence failed: {failure_message}"` | — | — | Remove — service logs it (Principle 3) |
| `f"Pipeline run complete..."` | `execution_finished` | info | `status="SUCCEEDED"`, `metadata={"report_status": report["summary"]["status"]}` |

**4.** Drop `execution_id` and `correlation_id` kwargs from `handle_phase_metrics_event` and `handle_failure_event` calls in `orchestration_event_handlers.py`.

**5.** Tests: add `autouse` fixture in `test_orchestration_event_handlers.py` for `clear_execution_context` cleanup. In tests that assert `execution_id` or `correlation_id` in the log output, call `set_execution_context(state.execution_id, state.correlation_id)` before invoking the handler.

---

### `validate_positions_quality.py`, `validate_trips_quality.py`, `create_quality_report.py` (json.dumps bridge — mechanical swap)

These already emit `json.dumps({"event": e, "message": m, "metadata": r})`. The replacement is one-to-one:

- Replace `import logging` + `logger = logging.getLogger(__name__)` with `from observability.structured_event_logger import get_structured_logger` + `structured_logger = get_structured_logger(logger_name=__name__)`
- Replace every `logger.info(json.dumps({"event": e, "message": m, "metadata": r}))` with `structured_logger.info(event=e, message=m, metadata=r)`
- Replace every `logger.warning(json.dumps(...))` with `structured_logger.warning(...)`
- Remove `import json` if it is no longer used after the swap

---

### `get_recent_positions.py`

Replace `import logging` + `logger = logging.getLogger(__name__)` with `get_structured_logger(logger_name=__name__)`.

Consolidate the 8 scattered calls into 3 structured events:

| Calls consolidated | Event | Level | Metadata |
|---|---|---|---|
| `"Bulk loading..."`, `f"Current hour: {current_hour}"`, `f"Minimum hour: {min_hour}"`, `"Connecting to DuckDB..."`, `f"Retrieveing position records..."`, `"Executing SQL query..."` | `positions_query_started` | info | `{"hours_interval": hours_interval, "current_hour": current_hour, "min_hour": min_hour, "s3_path": s3_path}` |
| `f"Retrieved {total_records}..."`, `"DuckDB connection closed."` | `positions_query_completed` | info | `{"record_count": total_records}` |
| `logger.error(error_message)` | `positions_query_failed` | error | — |

---

### `get_all_finished_trips.py`

Replace `import logging` + `logger = logging.getLogger(__name__)` with `get_structured_logger(logger_name=__name__)`.

| Old calls | Event | Level | Metadata |
|---|---|---|---|
| `f"Progress: {num_processed}..."` (inside loop, every 500) | `trip_extraction_progress` | info | `{"vehicle_line_groups_processed": num_processed}` |
| `f"Progress: {num_processed}..."` (after loop), `"Total finished trips..."`, `"Total invalid position records..."`, `"Trip extraction metrics..."` | `trip_extraction_completed` | info | `extraction_metrics` dict |

---

### `save_finished_trips_to_db.py`

Replace `import logging` + `logger = logging.getLogger(__name__)` with `get_structured_logger(logger_name=__name__)`.

| Old calls | Event | Level | Metadata |
|---|---|---|---|
| `f"Using staging table: {staging_table}..."` | `trips_save_started` | info | `{"staging_table": staging_table}` |
| `f"Filtered {len(...)} already-persisted trips..."` | `trips_filtered` | info | `{"filtered_count": len(trips_tuples) - len(new_trips_tuples), "new_count": len(new_trips_tuples)}` |
| `f"Sync complete: {added_rows} trips added..."` | `trips_saved` | info | `{"added_rows": added_rows, "previously_saved_rows": previously_saved_rows}` |
| `logger.error(error_message)` | `trips_save_failed` | error | — |

---

### `extract_trips_per_line_per_vehicle.py`

Replace `import logging` + `logger = logging.getLogger(__name__)` with `get_structured_logger(logger_name=__name__)`.

The error call is a production observability event — replace it:

| Old call | Event | Level | Metadata |
|---|---|---|---|
| `logger.error(error_message)` | `vehicle_trip_extraction_failed` | error | `{"linha_lt": linha_lt, "veiculo_id": veiculo_id}` |

The debug calls are algorithm traces, not observability events. Called thousands of times per run. Wrap each with `if logger.isEnabledFor(logging.DEBUG)` to eliminate argument evaluation overhead when debug is off. Do not convert to structured events.

```python
if logger.isEnabledFor(logging.DEBUG):
    logger.debug(f"No positions for line {linha_lt} vehicle {veiculo_id}")
```

---

### `extract_trips_from_positions.py`

Keep `import logging` and `logger = logging.getLogger(__name__)` — this file has only algorithm trace calls, no production observability events. Wrap all three debug calls with `if logger.isEnabledFor(logging.DEBUG)`. Do not convert to structured events.

---

**All existing tests pass.**

---

## Step 2 — NEW FEATURE: `execution_aborted`

**Purely additive. No existing emissions removed or moved.**

Define a helper inside `extract_trips.py`:

```python
def create_execution_aborted_log_record(message: str, phase: str) -> None:
    structured_logger.error(
        event="execution_aborted",
        message=f"Pipeline aborted: {message}",
        status="FAILED",
        metadata={"phase": phase},
    )
```

At every failure exit in `extract_trips.py`, after `handle_phase_metrics_event` and before `raise`, call:

```python
create_execution_aborted_log_record("<phase-specific message>", phase="<phase>")
```

| Phase | Message |
|---|---|
| `config_load` | `"Config load failed."` |
| `positions_load` | `"Positions load failed."` |
| `positions_quality` | `"Positions quality check failed."` |
| `trip_extraction` | `failure_message` |
| `persistence` | `failure_message` |
| `quality_report` | `"Quality report failed."` |

Add test `test_execution_aborted_event_emitted_on_pipeline_failure` asserting `event == "execution_aborted"`, `status`, `metadata["phase"]`, `execution_id`, and `correlation_id` are present. **All existing tests pass.**

---

## Step 3 — NEW FEATURE: `quality_report_metrics`

**Purely additive.**

After `create_final_quality_report` completes successfully in `extract_trips.py`, emit:

```python
structured_logger.info(
    event="quality_report_metrics",
    message="Quality report metrics",
    status="SUCCEEDED",
    metadata=report["summary"],
)
```

Update `handle_failure_event` in `orchestration_event_handlers.py` to emit `quality_report_metrics` (with `status="FAILED"`, `metadata=failure_report_summary`) after `create_failure_quality_report` succeeds.

Add test asserting `quality_report_metrics` is emitted on a successful run. **All existing tests pass.**
