# refinedfinishedtrips — Structured Logging Migration Plan v2

Migration not yet started.

**Guiding rule:** one concern per step, categorised as **NEW INFRA**, **REMOVED**, **REPLACEMENT**, or **NEW FEATURE**. All existing tests must pass after every step, except where a step explicitly updates tests as its own deliverable. No step mixes categories.

---

## Out of scope — private helpers are never decomposed

The following module-level helpers remain intact in every step. Their internal branching (`if status == "FAIL"`, `if status == "WARN"`, `raise`) is never modified:

- `_load_pipeline_config` — wraps config load with error logging
- `_parse_trip_extraction_output` — pure; no logging; unchanged
- `_build_column_lineage` — pure; no logging; unchanged
- `_handle_positions_result` — FAIL → call failure report + raise; WARN → call early report
- `_handle_trips_result` — WARN → log only
- `_handle_persistence_result` — WARN → log only

After Steps 2 and 5, the helpers will contain no webhook code and will emit structured log events, but their structure and branching is frozen.

---

## Behavioral contract (locked by existing tests — must hold after every step)

| Test | Contract |
|---|---|
| `test_positions_fail_raises_and_save_not_called` | `ValueError` raised; save not called |
| `test_positions_fail_calls_create_failure_report` | `create_failure_report_fn` called with `failure_phase="positions"` |
| `test_positions_fail_final_report_not_called` | `create_final_report_fn` never called on FAIL |
| `test_positions_warn_pipeline_continues_and_save_called` | WARN does not stop pipeline; save called |
| `test_trip_extraction_failure_calls_create_failure_report_with_positions_result` | failure report called with `positions_result`, `trips_result=None`, `persistence_result=None` |
| `test_persistence_failure_calls_create_failure_report_with_partial_results` | failure report called with `positions_result`, `trips_result`, `persistence_result=None`, `column_lineage` |
| `test_all_phases_pass_final_report_status_pass` | `create_final_report_fn` called with all three results and `column_lineage` |
| `test_trip_extraction_metrics_reach_final_report` | `extraction_metrics` tuple return reaches `validate_trips_fn` and the final report |

---

## Step 1 — NEW INFRA: create four new modules (zero changes to any existing file)

Create the following files. Do not touch `extract_trips.py` or any test file.

### `domain/events.py`

`_OrchestratorEvent` and `_ServiceEvent` TypeAliases covering the full event set:

| Event | When emitted |
|---|---|
| `execution_started` | top of orchestrator |
| `execution_finished` | success exit |
| `execution_failed` | every failure exit |
| `execution_phase_metrics` | every exit (success and failure) |
| `config_load_started/succeeded/failed` | config load phase |
| `positions_load_started/succeeded/failed` | positions load phase |
| `positions_quality_started/succeeded/failed/warned` | positions quality phase |
| `trip_extraction_started/succeeded/failed/warned` | trip extraction phase |
| `persistence_started/succeeded/failed/warned` | persistence phase |
| `quality_report_started/succeeded/failed` | quality report phase |
| `failure_report_error` | if failure report generation raises |

### `domain/logger.py`

`RefinedFinishedTripsLogger` wrapping `StructuredEventLogger`, narrowing `event: str` → `event: _OrchestratorEvent | _ServiceEvent`.

### `orchestration_dependencies.py`

`RefinedFinishedTripsOrchestrationDependencies` frozen dataclass. No `send_webhook` field.

Fields: `get_config`, `get_recent_positions`, `get_all_finished_trips`, `validate_positions_quality`, `validate_trips_quality`, `validate_persistence_quality`, `save_finished_trips_to_db`, `create_quality_report`, `create_failure_quality_report`, `create_final_quality_report`

### `orchestration_event_handlers.py`

**`PipelineTaskRunState`** — mutable dataclass (not frozen; fields accumulate during execution):

`execution_id`, `correlation_id`, `run_ts`, `pipeline_config` (default `None`), `positions_result` (default `None`), `trips_result` (default `None`), `persistence_result` (default `None`), `column_lineage` (default `None`), `extraction_metrics` (default `{}`)

**`handle_phase_metrics_event(state, tracker, structured_logger, overall_status)`** — emits `execution_phase_metrics` event via `structured_logger`.

**`handle_failure_event(state, deps, structured_logger, phase, message)`** — calls `deps.create_failure_quality_report(config=state.pipeline_config, execution_id=state.execution_id, run_ts=state.run_ts, failure_phase=phase, failure_message=message, positions_result=state.positions_result, trips_result=state.trips_result, persistence_result=state.persistence_result, column_lineage=state.column_lineage)`. On exception, emits `failure_report_error` and swallows.

`handle_failure_event` is **not** used inside `_handle_positions_result`. That helper calls its `create_failure_report_fn` argument directly (the positions FAIL report is inline, not wrapped in error handling).

Add unit tests for each new module. **All existing orchestration tests pass unchanged.**

---

## Step 2 — REMOVED: webhook

**Only removes.** No logic additions or substitutions.

### `extract_trips.py`

- Remove `from infra.notifications import send_webhook`
- Remove `send_webhook_fn: Callable[..., Any] = send_webhook` from function signature
- Remove the entire `_send_webhook_from_report` function
- Remove the `send_webhook_fn` parameter and both `_send_webhook_from_report(...)` calls from `_handle_positions_result`
- Remove `_send_webhook_from_report(...)` from the `trip_extraction` except block
- Remove `_send_webhook_from_report(...)` from the `persistence` except block

### `test_extract_trips_for_all_lines.py`

- Remove `noop_send_webhook` stub
- Remove `send_webhook_fn=noop_send_webhook` from every call site (10 tests use it as a passthrough)
- **Delete** `test_positions_fail_calls_send_webhook` — behavior removed
- `test_positions_warn_calls_create_report_and_early_webhook` → remove `webhooks` capture variable, remove `assert len(webhooks) == 2`; keep `assert len(reports) == 1`; rename to `test_positions_warn_calls_create_report`
- `test_all_phases_pass_final_webhook_sent_once` → remove `webhooks` capture, replace webhook lambda with a call-counting `create_final_report_fn` capture; assert the final report function was called once; rename to `test_all_phases_pass_final_report_called`

**All remaining behavioral assertions unchanged.**

---

## Step 3 — REPLACEMENT: individual callable parameters → deps dataclass

**Only changes how dependencies are passed into the orchestrator and into helpers. Zero logic changes.**

### `extract_trips.py`

Replace the 10 individual callable parameters with `deps: RefinedFinishedTripsOrchestrationDependencies`.

Internal call renames (one-to-one mechanical substitution):

| Before | After |
|---|---|
| `get_recent_positions_fn(config)` | `deps.get_recent_positions(config)` |
| `extract_trips_fn(config, df)` | `deps.get_all_finished_trips(config, df)` |
| `validate_positions_fn(config, df)` | `deps.validate_positions_quality(config, df)` |
| `validate_trips_fn(config, df, trips, extraction_metrics)` | `deps.validate_trips_quality(config, df, trips, extraction_metrics)` |
| `save_trips_fn(config, trips)` | `deps.save_finished_trips_to_db(config, trips)` |
| `validate_persistence_fn(save_result)` | `deps.validate_persistence_quality(save_result)` |
| `create_report_fn(...)` passed to `_handle_positions_result` | `deps.create_quality_report` passed as argument |
| `create_failure_report_fn(...)` in `trip_extraction` and `persistence` except blocks | `deps.create_failure_quality_report(...)` |
| `create_failure_report_fn` passed to `_handle_positions_result` | `deps.create_failure_quality_report` passed as argument |
| `create_final_report_fn(...)` | `deps.create_final_quality_report(...)` |

`_handle_positions_result` still receives individual callables as parameters — the orchestrator simply passes `deps.create_quality_report` and `deps.create_failure_quality_report` to it. The helper's signature and internal logic are unchanged.

### `tests/fakes/`

Create `FakeRefinedFinishedTripsOrchestrationDependencies` with a `create_scenario(**overrides)` factory that returns `(deps, recorder)`. Replace all inline lambda injection in `test_extract_trips_for_all_lines.py` with `create_scenario(**overrides)`. All behavioral assertions remain identical.

**All tests pass.**

---

## Step 4 — REPLACEMENT: `tracker.emit(logger, status)` → `handle_phase_metrics_event`

**Only changes the 7 call sites of `tracker.emit`. Nothing else.**

At the top of the function body, before any logic:

```python
structured_logger = logging.getLogger(__name__)  # still old-style; replaced in Step 5
state = SimpleNamespace(execution_id=execution_id, correlation_id=execution_id)  # stub; replaced in Step 5
```

Replace every `tracker.emit(logger, status)` with `handle_phase_metrics_event(state, tracker, structured_logger, status)`.

Call sites (7 total):
1. `config_load` except block
2. `positions_load` except block
3. `positions_quality` except block
4. `trip_extraction` except block (after failure report call)
5. `persistence` except block (after failure report call)
6. `quality_report` success path
7. `quality_report` except block

**All tests pass unchanged.**

---

## Step 5 — REPLACEMENT: logger → structured logger + full PipelineTaskRunState

**Replaces every old-style log call with structured equivalents and upgrades the state stub. Zero flow changes, zero branching changes, zero raise changes.**

### `extract_trips.py`

Remove `import logging` and `logger = logging.getLogger(__name__)`.

Replace the `SimpleNamespace` stub with `PipelineTaskRunState`:

```python
state = PipelineTaskRunState(
    execution_id=execution_id,
    correlation_id=execution_id,
    run_ts=run_ts,
)
```

Set `state.pipeline_config = config` immediately after config load succeeds.  
Set `state.positions_result`, `state.trips_result`, `state.persistence_result`, `state.column_lineage`, `state.extraction_metrics` as each becomes available (same points where the local variables are currently assigned).

Replace `structured_logger = logging.getLogger(__name__)` with:

```python
structured_logger = RefinedFinishedTripsLogger(service_name="refinedfinishedtrips", execution_id=execution_id, correlation_id=execution_id)
```

Replace every `logger.info/warning/error(...)` in the main function body with the equivalent structured call using the event taxonomy. The message text stays identical; only the call form changes.

In `trip_extraction` except block: replace `logger.error(f"Trip extraction failed: ...") + deps.create_failure_quality_report(...)` with `structured_logger.error(event="trip_extraction_failed", ...) + handle_failure_event(state, deps, structured_logger, "trip_extraction", failure_message)`.

In `persistence` except block: same pattern with `persistence_failed` + `handle_failure_event`.

### Private helpers (signature change only — no logic change)

Each helper that uses `logger` receives `structured_logger` as a new final parameter. The module-level `logger = logging.getLogger(__name__)` is removed. Branching, raises, and service function calls inside each helper are untouched.

| Helper | New parameter | Log calls replaced |
|---|---|---|
| `_load_pipeline_config` | `structured_logger` | `logger.error(...)` → `structured_logger.error(event="config_load_failed", ...)` |
| `_handle_positions_result` | `structured_logger` | `logger.error(...)` → `positions_quality_failed`; `logger.warning(...)` → `positions_quality_warned` |
| `_handle_trips_result` | `structured_logger` | `logger.warning(...)` → `trip_extraction_warned` |
| `_handle_persistence_result` | `structured_logger` | `logger.warning(...)` → `persistence_warned` |

The `create_failure_report_fn(config, execution_id, run_ts, "positions", failure_message, positions_result)` call inside `_handle_positions_result` is **unchanged**. The `raise ValueError(...)` after it is **unchanged**. The `if status == "FAIL"` / `if status == "WARN"` branches are **unchanged**.

### New file

Add `config/observability.py` with `THIRD_PARTY_LOGGER_NAMESPACES`.

**All existing tests pass.**

---

## Step 6 — NEW FEATURE: `execution_started`, `execution_finished`, `execution_failed`

**Purely additive. No existing emissions removed or moved.**

Define inner closure at the very top of the function body, before any logic line:

```python
def create_execution_failed_log_record(message: str) -> None:
    structured_logger.error(
        event="execution_failed",
        message=message,
        execution_id=state.execution_id,
        correlation_id=state.correlation_id,
        status="FAILED",
    )
```

Add `structured_logger.info(event="execution_started", ...)` at the very top of the function body (before `tracker` and `state` are even constructed — this is the first action).

Add `structured_logger.info(event="execution_finished", ...)` just before the final `return`.

At every failure exit (after `handle_phase_metrics_event`, before `raise`): call `create_execution_failed_log_record(message)`. The message per exit point:

| Phase | Message |
|---|---|
| `config_load` | `"Config load failed."` |
| `positions_load` | `"Positions load failed."` |
| `positions_quality` | `"Positions quality check failed."` |
| `trip_extraction` | `str(exc)` (same as `failure_message`) |
| `persistence` | `str(exc)` (same as `failure_message`) |
| `quality_report` | `"Quality report failed."` |

**No existing log calls removed or moved.**

Add test: `test_execution_failed_event_is_emitted_on_pipeline_failure` asserting `event`, `status`, `message`, `execution_id`, `correlation_id` are present. **All existing tests pass.**

---

## Step 7 — NEW FEATURE: `quality_report_metrics`

**Purely additive.**

Add a `quality_report_metrics` structured log event at the end of a successful run, after `create_final_quality_report` completes, logging the final report's summary as `metadata`. Mirror the structure used in `transformlivedata`.

Update `handle_failure_event` to emit `quality_report_metrics` (with `status="FAILED"`, `metadata=failure_report_summary`) after `create_failure_quality_report` succeeds. This covers all failure exits that reach the failure report.

**All existing tests pass.** Add test asserting the event is emitted on a successful run.
