# Quality Checking & Reporting — refinedfinishedtrips

## Spec

### Principles

- No Great Expectations — all checks are pure pandas computations
- Each pipeline phase produces its own named validation result
- Each check returns a structured result: `{check, status, observed, threshold}`
- Overall status = worst status across all phases (FAIL > WARN > PASS)
- `items_failed` in summary = count of FAIL-status checks only
- Validation functions accept an injectable `now_fn` (defaults to `lambda: datetime.now(timezone.utc)`) for deterministic testing

### Timezone reference

| Field | Timezone |
|---|---|
| `veiculo_ts` | tz-naive, values in America/Sao Paulo |
| `extracao_ts` | UTC tz-aware |
| `now()` Airflow | UTC tz-aware |
| `now()` local dev | America/Sao Paulo tz-aware |

### Phase 1 — Positions read (`get_recent_positions`)

**Behaviour on result:**
- FAIL → pipeline stops immediately, failure report emitted, webhook sent
- WARN → pipeline continues, warning carried into final report

**Prerequisite change:** add `extracao_ts` to the `SELECT` in `get_recent_positions`

**Check 1: Freshness** — uses `veiculo_ts` (catches SPTrans not updating positions even when extraction is still running)
- Inside check: convert `now()` → America/Sao Paulo → strip tz → compare with tz-naive `max(veiculo_ts)`
- Empty DataFrame → FAIL, note: "no positions available for the analysis time window"
- WARN if lag > `freshness_warn_staleness_minutes`
- FAIL if lag > `freshness_fail_staleness_minutes`
- `warn < fail`

**Check 2: Recent gaps** — uses `extracao_ts` (catches extraction service failures)
- Extract `DISTINCT extracao_ts`, sort, compute consecutive diffs, scoped to last `gaps_recent_window_minutes`
- `now()` is UTC-aware, `extracao_ts` is UTC-aware → direct comparison
- Fewer than 2 distinct values in recent window → FAIL with descriptive note
- WARN if max gap > `gaps_warn_gap_minutes`
- FAIL if max gap > `gaps_fail_gap_minutes`
- `warn < fail`

**New config keys (under `quality`):**
```
freshness_warn_staleness_minutes
freshness_fail_staleness_minutes
gaps_warn_gap_minutes
gaps_fail_gap_minutes
gaps_recent_window_minutes
```

### Phase 2 — Trip extraction (`get_all_finished_trips`)

**Behaviour on result:**
- WARN → pipeline continues, warning carried into final report

**Prerequisite metric:** `effective_window = max(veiculo_ts) - min(veiculo_ts)` of the full dataset

**Check 1: Zero trips despite sufficient data**
- WARN if `effective_window > trips_effective_window_threshold_minutes` AND `trips_extracted == 0`

**Check 2: Suspiciously low trip count**
- WARN if `effective_window > trips_effective_window_threshold_minutes` AND `trips_extracted < trips_min_trips_threshold`

**New config keys (under `quality`):**
```
trips_effective_window_threshold_minutes
trips_min_trips_threshold
```

### Phase 3 — Persistence (`save_finished_trips_to_db`)

**Behaviour on result:**
- WARN → pipeline continues, warning carried into final report

**Prerequisite change:** `save_finished_trips_to_db` returns `{"new_rows": int, "skipped_rows": int}` instead of `None`

**Check 1: All trips skipped as duplicates**
- WARN if `new_rows == 0` AND `skipped_rows > 0`

### Overall status derivation

| Phases result | Overall status |
|---|---|
| All PASS | PASS |
| Any WARN, no FAIL | WARN |
| Any FAIL | FAIL |

### Report structure

```json
{
  "summary": {
    "contract_version": "v1",
    "pipeline": "refinedfinishedtrips",
    "execution_id": "...",
    "status": "PASS",
    "failure_phase": null,
    "failure_message": null,
    "items_failed": 0,
    "quality_report_path": "metadata/quality-reports/refinedfinishedtrips/...",
    "generated_at_utc": "...",
    "positions_in_time_window_count": 150000,
    "trips_extracted": 1247,
    "new_trips_saved": 245,
    "skipped_trips": 1002
  },
  "details": {
    "execution_id": "...",
    "status": "PASS",
    "failure_phase": null,
    "failure_message": null,
    "phases": {
      "positions": {
        "status": "PASS",
        "positions_in_time_window_count": 150000,
        "checks": [
          {
            "check": "freshness",
            "status": "PASS",
            "observed_lag_minutes": 3.2,
            "warn_threshold_minutes": 10,
            "fail_threshold_minutes": 20
          },
          {
            "check": "recent_gaps",
            "status": "PASS",
            "max_gap_minutes": 2.1,
            "warn_threshold_minutes": 5,
            "fail_threshold_minutes": 10,
            "recent_window_minutes": 60
          }
        ]
      },
      "trip_extraction": {
        "status": "PASS",
        "analysis_window_minutes": 180,
        "effective_window_minutes": 178.3,
        "trips_extracted": 1247,
        "checks": [
          {
            "check": "zero_trips",
            "status": "PASS",
            "effective_window_minutes": 178.3,
            "window_threshold_minutes": 60
          },
          {
            "check": "low_trip_count",
            "status": "PASS",
            "trips_extracted": 1247,
            "min_trips_threshold": 10
          }
        ]
      },
      "persistence": {
        "status": "PASS",
        "new_rows": 245,
        "skipped_rows": 1002
      }
    },
    "artifacts": {
      "quality_report_path": "metadata/quality-reports/refinedfinishedtrips/..."
    }
  }
}
```

---

## Implementation Plan

### Mandatory workflow

Each phase follows this sequence without exception:
1. Implement all changes for the phase
2. Write and run tests
3. Present results and wait for explicit approval
4. Only then proceed to the next phase

---

### Phase 1 — Positions quality (`get_recent_positions`)

**Config changes (atomic — only what Phase 1 needs)**
- Add `QualityConfig` to `refinedfinishedtrips_config_schema.py` with:
  `freshness_warn_staleness_minutes`, `freshness_fail_staleness_minutes`, `gaps_warn_gap_minutes`, `gaps_fail_gap_minutes`, `gaps_recent_window_minutes`
- Add `metadata_bucket`, `quality_report_folder` to `StorageConfig`
- Add `NotificationsConfig` with `webhook_url` to `GeneralConfig`
- Add all new keys to `refinedfinishedtrips_general.json`

**`get_recent_positions`**
- Add `extracao_ts` to the `SELECT`

**New service: `validate_positions_quality.py`**
- `check_freshness(df, config, now_fn)` → structured check result
  - Empty DataFrame → FAIL, note: "no positions available for the analysis time window"
  - Convert `now()` → America/Sao Paulo → strip tz → compare with `max(veiculo_ts)`
- `check_recent_gaps(df, config, now_fn)` → structured check result
  - `DISTINCT extracao_ts` scoped to last `gaps_recent_window_minutes`
  - Fewer than 2 values → FAIL with descriptive note
- `validate_positions_quality(df, config, now_fn)` → `{status, checks: [...]}`
  - Overall status = worst of both checks

**New service: `create_quality_report.py`**
- `build_quality_report(execution_id, run_ts, positions_result, config)` → full `{summary, details}` envelope following the agreed report structure
- `create_quality_report(config, ..., write_fn)` → builds + saves to MinIO following gtfs path pattern
- `create_failure_quality_report(config, execution_id, run_ts, failure_phase, failure_message, positions_result, write_fn)` → failure envelope + saves to MinIO

**Wiring: `extract_trips_for_all_Lines_and_vehicles.py`**
- `run_ts = datetime.now(timezone.utc)` captured at entry
- Remove the current empty DataFrame early-return guard
- New injectables: `validate_positions_fn`, `create_report_fn`, `create_failure_report_fn`, `send_webhook_fn`
- Pseudocode:
```
run_ts = now()
df = get_recent_positions_fn(config)
positions_result = validate_positions_fn(df, config)
if positions_result.status == FAIL:
    report = create_failure_report_fn(config, execution_id, run_ts, ...)
    send_webhook_fn(report.summary, webhook_url)
    raise
# continue to phase 2 (extract + save, no quality yet)
```

**Alertservice**
- Add `refinedfinishedtrips` entry to `alertservice/config/pipelines.yaml` following existing pipeline structure

**Tests: `validate_positions_quality`**
- Freshness PASS, WARN, FAIL
- Freshness with empty DataFrame → FAIL
- Recent gaps PASS, WARN, FAIL
- Recent gaps with fewer than 2 distinct `extracao_ts` → FAIL
- Timezone normalization: `now_fn` injected as UTC-aware, verified against tz-naive `veiculo_ts`
- `validate_positions_quality` overall status derivation

**Tests: `create_quality_report`**
- Report structure matches agreed envelope
- `items_failed` counts only FAIL checks
- Failure report structure
- MinIO save called with correct path

**→ Wait for approval before proceeding to Phase 2**

---

### Phase 2 — Trip extraction quality (`get_all_finished_trips`)

**Config changes (atomic — only what Phase 2 needs)**
- Add `trips_effective_window_threshold_minutes`, `trips_min_trips_threshold` to `QualityConfig` in schema and JSON

**New service: `validate_trips_quality.py`**
- `validate_trips_quality(df, trips_extracted, config)` → `{status, checks: [...]}`
  - `effective_window = max(veiculo_ts) - min(veiculo_ts)`
  - Check zero trips, check low trip count

**Wiring: `extract_trips_for_all_Lines_and_vehicles.py`**
- Add `validate_trips_fn` injectable
- After `extract_trips_fn`: run `validate_trips_fn`, accumulate result
- Pass trips result into final report

**Tests: `validate_trips_quality`**
- Zero trips with sufficient effective window → WARN
- Zero trips with insufficient effective window → PASS
- Low trip count with sufficient window → WARN
- Adequate trip count → PASS

**→ Wait for approval before proceeding to Phase 3**

---

### Phase 3 — Persistence quality (`save_finished_trips_to_db`)

**Config changes**
- None needed

**`save_finished_trips_to_db`**
- Return `{"new_rows": int, "skipped_rows": int}` instead of `None`

**New service: `validate_persistence_quality.py`**
- `validate_persistence_quality(save_result)` → `{status, new_rows, skipped_rows}`
  - All duplicates → WARN

**Wiring: `extract_trips_for_all_Lines_and_vehicles.py`**
- Add `validate_persistence_fn` injectable
- After `save_trips_fn`: run `validate_persistence_fn`, accumulate result
- All three phase results now available → derive overall status → build and save final report → send webhook

**Tests: `validate_persistence_quality`**
- All duplicates → WARN
- Mix of new and duplicates → PASS
- All new → PASS

**Tests: end-to-end wiring**
- All phases PASS → report status PASS, webhook sent once
- Phase 1 WARN + others PASS → report status WARN
- Phase 1 FAIL → pipeline stops, failure report sent, phases 2 and 3 not executed
- `save_finished_trips_to_db` return contract verified

**→ Wait for approval — implementation complete**
