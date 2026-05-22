# refinedfinishedtrips — Orchestration Refactoring Plan

## Baseline

The following are already complete and are not part of this plan:

- `_count_failed_checks` NameError fixed (promoted to module level in `services/create_quality_report.py`)
- `PipelineTaskRunState` exists in `orchestration_event_handlers.py` with all result fields
- `handle_failure_event` correctly passes state fields to `create_failure_quality_report`
- config_load except block is complete (`tracker.finish` + `handle_phase_metrics_event`)

Structured logging migration resumes **after Phase 4**.

---

## Phase 1 — Remove dead code

**Goal:** eliminate coupling and dead paths. No architecture changes. Pipeline must run end-to-end with all tests passing.

### `services/create_quality_report.py`
- Delete `create_quality_report` function

### `orchestration_dependencies.py`
- Remove `create_quality_report` field

### `extract_trips.py`
- Remove `_handle_trips_result` function and its call site
- Remove `_handle_persistence_result` function and its call site
- Remove `create_report_fn` and `create_failure_report_fn` parameters from `_handle_positions_result`; remove both calls inside it
- Update the call site of `_handle_positions_result`: drop the two dep arguments

### Tests
- `test_orchestration_dependencies.py`: remove `create_quality_report` from `_ALL_FIELDS`
- `test_orchestration_event_handlers.py`: remove `create_quality_report=_noop` from `_make_failure_deps`
- `test_extract_trips_for_all_lines.py`:
  - `test_positions_fail_calls_create_failure_report` → `test_positions_fail_no_failure_report_called`, assert `recorder.failure_report_calls == []`
  - `test_positions_warn_calls_create_report` → `test_positions_warn_no_early_report_called`, assert `recorder.early_report_calls == []`
- `tests/fakes/fake_refinedfinishedtrips_orchestration_dependencies.py`: remove `create_quality_report` from deps constructor

### Deliverable
`create_quality_report` dep and service gone; positions FAIL raises immediately with no report; positions WARN continues silently; trips and persistence failure reports still generated via inline calls; all tests pass.

---

## Phase 2 — Wire `PipelineTaskRunState` into the orchestrator

**Goal:** replace `SimpleNamespace`, accumulate state at each phase, route all failure reports through `handle_failure_event`. No changes to validate functions or quality report services.

### `services/create_quality_report.py`
- `create_failure_quality_report`: change `positions_result: Dict[str, Any]` → `positions_result: Optional[Dict[str, Any]] = None`

### `extract_trips.py`
- Import `PipelineTaskRunState` and `handle_failure_event` from `orchestration_event_handlers`
- Replace `state = SimpleNamespace(...)` with `PipelineTaskRunState(execution_id=..., correlation_id=..., run_ts=run_ts)`
- After config load success: `state.pipeline_config = pipeline_config`
- positions_load except block: add `handle_failure_event(state, deps, structured_logger, "positions_load", str(exc))` before `handle_phase_metrics_event`
- positions_quality block: set `state.positions_result = positions_result` **before** calling `_handle_positions_result`; add `handle_failure_event(state, deps, structured_logger, "positions_quality", str(exc))` to except block before `handle_phase_metrics_event`
- trip_extraction block: set `state.column_lineage = column_lineage` and `state.trips_result = trips_result` after they are computed; replace inline `deps.create_failure_quality_report(...)` in except block with `handle_failure_event(state, deps, structured_logger, "trip_extraction", failure_message)`
- persistence block: set `state.persistence_result = persistence_result` after it is computed; replace inline `deps.create_failure_quality_report(...)` in except block with `handle_failure_event(state, deps, structured_logger, "persistence", failure_message)`

### Tests
- `test_extract_trips_for_all_lines.py`:
  - `test_positions_fail_no_failure_report_called` → `test_positions_fail_failure_report_called`: assert `recorder.failure_report_calls` has one entry with `failure_phase == "positions_quality"`
  - Verify trip_extraction and persistence failure tests still pass with the new routing

### Deliverable
Orchestrator uses proper state; every failure path calls `handle_failure_event`; failure reports carry all metrics accumulated to the point of failure; all tests pass.

---

## Phase 3a — Positions quality metrics reform

**Goal:** `validate_positions_quality` raises on hard failure, returns plain metrics on success. No status field, no checks array.

### `services/validate_positions_quality.py`
- `validate_positions_quality` raises `ValueError` with descriptive message when: df is empty, freshness lag > fail_threshold, max gap > fail_threshold
- On success returns plain metrics:
  ```python
  {
      "positions_in_time_window_count": int,
      "freshness_lag_minutes": float,
      "freshness_warn_threshold_minutes": float,
      "freshness_fail_threshold_minutes": float,
      "max_gap_minutes": float | None,
      "gaps_warn_threshold_minutes": float,
      "gaps_fail_threshold_minutes": float,
  }
  ```
- `check_freshness` and `check_recent_gaps` become private implementation details

### `extract_trips.py`
- Remove `_handle_positions_result` entirely (FAIL is now the exception from validate; WARN is detected by the quality report at the end)
- The positions_quality except block already calls `handle_failure_event` (wired in Phase 2) — no change needed

### `services/create_quality_report.py`
- Update `create_final_quality_report` and `create_failure_quality_report` to handle `positions_result` without `status` or `checks` fields

### Tests / Fake
- Fake `validate_positions_quality`: raises `ValueError` when `positions_status="FAIL"`, returns metrics dict on `"PASS"` or `"WARN"`
- Update tests that previously inspected `positions_result` status or checks

### Deliverable
`validate_positions_quality` raises on FAIL; `_handle_positions_result` gone; quality report accepts new positions metrics shape; all tests pass.

---

## Phase 3b — Trips quality metrics reform

**Goal:** `validate_trips_quality` returns plain metrics, no status, no checks.

### `services/validate_trips_quality.py`
- Returns:
  ```python
  {
      "effective_window_minutes": float,
      "trips_extracted": int,
      "source_sentido_discrepancies": int,
      "sanitization_dropped_points": int,
      "input_position_records": int,
      "vehicle_line_groups_processed": int,
  }
  ```
- Never raises (zero or low trip count is a reportable observation, not a pipeline failure)

### `services/create_quality_report.py`
- Update both report functions to handle `trips_result` without `status` or `checks`

### Tests / Fake
- Fake and tests updated for new trips metrics shape

### Deliverable
`validate_trips_quality` returns metrics only; quality report accepts new trips metrics shape; all tests pass.

---

## Phase 3c — Persistence quality metrics reform

**Goal:** `validate_persistence_quality` returns plain metrics, no status.

### `services/validate_persistence_quality.py`
- Returns:
  ```python
  {"added_rows": int, "previously_saved_rows": int}
  ```
- Never raises

### `services/create_quality_report.py`
- Update both report functions to handle `persistence_result` without `status`

### Tests / Fake
- Fake and tests updated

### Deliverable
`validate_persistence_quality` returns metrics only; quality report accepts new persistence metrics shape; all tests pass.

---

## Phase 4 — Rebuild quality report

**Goal:** quality report computes PASS/WARN/FAIL internally from metric values against config thresholds. No status inputs.

### `services/create_quality_report.py`
- Delete `build_quality_report` (status-based, obsolete after Phase 3)
- `create_final_quality_report(config, execution_id, run_ts, positions_result, trips_result, persistence_result, column_lineage)`: derives overall status by comparing metric values against config thresholds (freshness lag vs warn/fail threshold, trip count vs min threshold, etc.)
- `create_failure_quality_report(config, execution_id, run_ts, failure_phase, failure_message, positions_result, trips_result, persistence_result, column_lineage)`: always FAIL; records whatever partial metrics are non-None; no status derivation
- Move status derivation logic to a module-level private function that takes metrics + config

### Tests
- Full coverage for both functions: PASS, WARN, and FAIL scenarios using metrics-only inputs (no status fields)

### Deliverable
Quality report fully rebuilt; PASS/WARN/FAIL derived from thresholds not from inputs; `build_quality_report` removed; all tests pass.
