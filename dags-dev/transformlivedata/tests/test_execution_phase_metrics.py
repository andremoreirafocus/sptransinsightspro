import json
import logging

from quality.execution_phase_metrics import (
    ExecutionPhaseMetricsTracker,
)


PHASES = [
    "config_load",
    "load_positions",
    "raw_schema_validation",
]


def test_tracker_initializes_all_phases_as_skipped():
    tracker = ExecutionPhaseMetricsTracker(
        pipeline="transformlivedata",
        execution_id="exec-1",
        logical_date_utc="2026-05-17T10:00:00+00:00",
        phase_order=PHASES,
    )

    payload = tracker.to_log_payload("success")
    assert set(payload["phase_metrics"].keys()) == set(PHASES)
    for phase in PHASES:
        assert payload["phase_metrics"][phase]["status"] == "skipped"
        assert payload["phase_metrics"][phase]["duration_seconds"] == 0.0


def test_tracker_updates_phase_status_and_duration_on_finish():
    tracker = ExecutionPhaseMetricsTracker(
        pipeline="transformlivedata",
        execution_id="exec-1",
        logical_date_utc="2026-05-17T10:00:00+00:00",
        phase_order=PHASES,
    )

    tracker.begin("config_load")
    tracker.finish("config_load", "success")
    payload = tracker.to_log_payload("success")

    assert payload["phase_metrics"]["config_load"]["status"] == "success"
    assert payload["phase_metrics"]["config_load"]["duration_seconds"] >= 0.0


def test_tracker_payload_contains_contract_fields():
    tracker = ExecutionPhaseMetricsTracker(
        pipeline="transformlivedata",
        execution_id="exec-1",
        logical_date_utc="2026-05-17T10:00:00+00:00",
        phase_order=PHASES,
    )
    payload = tracker.to_log_payload("failed")

    assert payload["event"] == "execution_phase_metrics"
    assert payload["pipeline"] == "transformlivedata"
    assert payload["execution_id"] == "exec-1"
    assert payload["logical_date_utc"] == "2026-05-17T10:00:00+00:00"
    assert payload["overall_status"] == "failed"
    assert "total_duration_seconds" in payload
    assert "phase_metrics" in payload


def test_tracker_emit_logs_payload_as_json(caplog):
    tracker = ExecutionPhaseMetricsTracker(
        pipeline="transformlivedata",
        execution_id="exec-1",
        logical_date_utc="2026-05-17T10:00:00+00:00",
        phase_order=PHASES,
    )
    logger = logging.getLogger("test.execution_phase_metrics")
    caplog.set_level("INFO", logger=logger.name)

    tracker.emit(logger, "success")

    payloads = []
    for record in caplog.records:
        try:
            payloads.append(json.loads(record.getMessage()))
        except Exception:
            continue

    assert len(payloads) == 1
    assert payloads[0]["event"] == "execution_phase_metrics"
    assert payloads[0]["overall_status"] == "success"
