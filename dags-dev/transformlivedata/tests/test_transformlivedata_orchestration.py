import json

import pandas as pd
import pytest

from transformlivedata.tests.fakes import (
    FakeTransformLiveDataOrchestrationDependencies,
)
from transformlivedata.transformlivedata import load_transform_save_positions


PHASES = [
    "config_load",
    "load_positions",
    "raw_schema_validation",
    "transform",
    "expectations_validation",
    "save_trusted",
    "save_quarantine",
    "mark_processed",
    "quality_report",
]


def _extract_quality_report_metrics_events(caplog):
    payloads = []
    for record in caplog.records:
        message = record.getMessage()
        try:
            parsed = json.loads(message)
        except Exception:
            continue
        if parsed.get("event") == "quality_report_metrics":
            payloads.append(parsed)
    return payloads


def _extract_execution_aborted_events(caplog):
    payloads = []
    for record in caplog.records:
        message = record.getMessage()
        try:
            parsed = json.loads(message)
        except Exception:
            continue
        if parsed.get("event") == "execution_aborted":
            payloads.append(parsed)
    return payloads


def _extract_execution_phase_metrics_log(caplog):
    payloads = []
    for record in caplog.records:
        message = record.getMessage()
        try:
            parsed = json.loads(message)
        except Exception:
            continue
        if parsed.get("event") == "execution_phase_metrics":
            payloads.append(parsed)
    assert len(payloads) == 1
    return payloads[0]


def test_orchestration_emits_success_execution_phase_metrics(caplog):
    deps, recorder = FakeTransformLiveDataOrchestrationDependencies.create_scenario()
    caplog.clear()
    caplog.set_level("INFO")

    load_transform_save_positions("transformlivedata", "2026-05-17T10:00:00+00:00", deps)

    payload = _extract_execution_phase_metrics_log(caplog)

    assert payload["metadata"]["overall_status"] == "success"
    assert payload["metadata"]["pipeline"] == "transformlivedata"
    assert payload["metadata"]["logical_date_utc"] == "2026-05-17T10:00:00+00:00"
    assert set(payload["metadata"]["phase_metrics"].keys()) == set(PHASES)
    assert "notify_webhook" not in payload["metadata"]["phase_metrics"]

    for phase in PHASES:
        phase_data = payload["metadata"]["phase_metrics"][phase]
        assert set(phase_data.keys()) == {"duration_seconds", "status"}

    assert payload["metadata"]["phase_metrics"]["save_quarantine"]["status"] == "skipped"
    assert recorder.mark_processed_calls == 1
    assert recorder.quality_report_calls == 1
    assert "save_positions_to_storage:trusted" in recorder.calls


def test_orchestration_emits_failed_execution_phase_metrics_when_load_fails(caplog):
    deps, recorder = FakeTransformLiveDataOrchestrationDependencies.create_scenario(
        load_raises=RuntimeError("storage unavailable")
    )
    caplog.clear()
    caplog.set_level("ERROR")

    with pytest.raises(RuntimeError, match="storage unavailable"):
        load_transform_save_positions(
            "transformlivedata",
            "2026-05-17T10:00:00+00:00",
            deps,
        )

    payload = _extract_execution_phase_metrics_log(caplog)

    assert payload["metadata"]["overall_status"] == "failed"
    assert payload["metadata"]["phase_metrics"]["config_load"]["status"] == "success"
    assert payload["metadata"]["phase_metrics"]["load_positions"]["status"] == "failed"
    assert payload["metadata"]["phase_metrics"]["transform"]["status"] == "skipped"
    assert payload["metadata"]["phase_metrics"]["quality_report"]["status"] == "skipped"

    for phase in PHASES:
        phase_data = payload["metadata"]["phase_metrics"][phase]
        assert set(phase_data.keys()) == {"duration_seconds", "status"}
    assert recorder.failure_quality_report_calls == 1


def test_raw_schema_validation_failure_raises_and_emits_metrics(caplog):
    deps, recorder = FakeTransformLiveDataOrchestrationDependencies.create_scenario(
        schema_valid=False
    )
    caplog.set_level("ERROR")

    with pytest.raises(ValueError, match="Raw data validation failed"):
        load_transform_save_positions("transformlivedata", "2026-05-17T10:00:00+00:00", deps)

    payload = _extract_execution_phase_metrics_log(caplog)
    assert payload["metadata"]["overall_status"] == "failed"
    assert payload["metadata"]["phase_metrics"]["raw_schema_validation"]["status"] == "failed"
    assert payload["metadata"]["phase_metrics"]["transform"]["status"] == "skipped"
    assert recorder.failure_quality_report_calls == 1


def test_raw_schema_validation_exception_propagates(caplog):
    deps, recorder = FakeTransformLiveDataOrchestrationDependencies.create_scenario(
        schema_raises=RuntimeError("schema load error")
    )
    caplog.set_level("ERROR")

    with pytest.raises(RuntimeError, match="schema load error"):
        load_transform_save_positions("transformlivedata", "2026-05-17T10:00:00+00:00", deps)

    payload = _extract_execution_phase_metrics_log(caplog)
    assert payload["metadata"]["overall_status"] == "failed"
    assert payload["metadata"]["phase_metrics"]["raw_schema_validation"]["status"] == "failed"
    assert payload["metadata"]["phase_metrics"]["transform"]["status"] == "skipped"

    aborted = _extract_execution_aborted_events(caplog)
    assert len(aborted) == 1
    assert aborted[0]["metadata"]["phase"] == "raw_schema_validation"
    assert recorder.failure_quality_report_calls == 1


def test_transform_empty_positions_fails_fast(caplog):
    deps, _ = FakeTransformLiveDataOrchestrationDependencies.create_scenario(
        transform_positions_df=pd.DataFrame()
    )
    caplog.set_level("ERROR")

    with pytest.raises(ValueError, match="No valid position records found"):
        load_transform_save_positions("transformlivedata", "2026-05-17T10:00:00+00:00", deps)

    payload = _extract_execution_phase_metrics_log(caplog)
    assert payload["metadata"]["phase_metrics"]["transform"]["status"] == "failed"
    assert payload["metadata"]["phase_metrics"]["expectations_validation"]["status"] == "skipped"


def test_expectations_validation_exception_skips_trusted_save(caplog):
    deps, recorder = FakeTransformLiveDataOrchestrationDependencies.create_scenario(
        expectations_raises=RuntimeError("gx failed")
    )
    caplog.set_level("ERROR")

    with pytest.raises(RuntimeError, match="gx failed"):
        load_transform_save_positions("transformlivedata", "2026-05-17T10:00:00+00:00", deps)

    payload = _extract_execution_phase_metrics_log(caplog)
    assert payload["metadata"]["phase_metrics"]["expectations_validation"]["status"] == "failed"
    assert payload["metadata"]["phase_metrics"]["save_trusted"]["status"] == "skipped"
    assert "save_positions_to_storage:trusted" not in recorder.calls


def test_save_trusted_failure_should_not_mark_processed(caplog):
    deps, recorder = FakeTransformLiveDataOrchestrationDependencies.create_scenario(
        save_trusted_raises=RuntimeError("trusted save failed")
    )
    caplog.set_level("ERROR")

    with pytest.raises(RuntimeError, match="trusted save failed"):
        load_transform_save_positions("transformlivedata", "2026-05-17T10:00:00+00:00", deps)

    payload = _extract_execution_phase_metrics_log(caplog)
    assert payload["metadata"]["phase_metrics"]["save_trusted"]["status"] == "failed"
    assert recorder.failure_quality_report_calls == 1
    assert recorder.mark_processed_calls == 0


def test_save_quarantine_success_when_invalid_rows_exist(caplog):
    deps, recorder = FakeTransformLiveDataOrchestrationDependencies.create_scenario(
        transform_invalid_df=pd.DataFrame(
            [{"extracao_ts": "2026-05-17T10:00:00+00:00", "veiculo_id": 99}]
        )
    )
    caplog.set_level("INFO")

    load_transform_save_positions("transformlivedata", "2026-05-17T10:00:00+00:00", deps)

    payload = _extract_execution_phase_metrics_log(caplog)
    assert payload["metadata"]["phase_metrics"]["save_quarantine"]["status"] == "success"
    assert "quarantined" in recorder.save_calls


def test_mark_processed_failure_should_not_build_success_quality_report(caplog):
    deps, recorder = FakeTransformLiveDataOrchestrationDependencies.create_scenario(
        mark_processed_raises=RuntimeError("mark failed")
    )
    caplog.set_level("ERROR")

    with pytest.raises(RuntimeError, match="mark failed"):
        load_transform_save_positions("transformlivedata", "2026-05-17T10:00:00+00:00", deps)

    payload = _extract_execution_phase_metrics_log(caplog)
    assert payload["metadata"]["phase_metrics"]["mark_processed"]["status"] == "failed"
    assert recorder.quality_report_calls == 0


def test_quality_report_failure_emits_failed_terminal_metrics(caplog):
    deps, recorder = FakeTransformLiveDataOrchestrationDependencies.create_scenario(
        quality_report_raises=RuntimeError("report failed")
    )
    caplog.set_level("ERROR")

    with pytest.raises(RuntimeError, match="report failed"):
        load_transform_save_positions("transformlivedata", "2026-05-17T10:00:00+00:00", deps)

    payload = _extract_execution_phase_metrics_log(caplog)
    assert payload["metadata"]["overall_status"] == "failed"
    assert payload["metadata"]["phase_metrics"]["quality_report"]["status"] == "failed"
    assert recorder.quality_report_calls == 1


def test_orchestration_emits_quality_report_metrics_on_success(caplog):
    deps, _ = FakeTransformLiveDataOrchestrationDependencies.create_scenario()
    caplog.clear()
    caplog.set_level("INFO")

    load_transform_save_positions("transformlivedata", "2026-05-17T10:00:00+00:00", deps)

    events = _extract_quality_report_metrics_events(caplog)
    assert len(events) == 1
    event = events[0]
    assert event["status"] == "SUCCEEDED"
    assert "execution_phase_metrics" not in event.get("metadata", {})


def test_execution_aborted_event_is_emitted_on_pipeline_failure(caplog):
    deps, _ = FakeTransformLiveDataOrchestrationDependencies.create_scenario(
        load_raises=RuntimeError("storage unavailable")
    )
    caplog.set_level("ERROR")

    with pytest.raises(RuntimeError):
        load_transform_save_positions("transformlivedata", "2026-05-17T10:00:00+00:00", deps)

    events = _extract_execution_aborted_events(caplog)
    assert len(events) == 1
    event = events[0]
    assert event["event"] == "execution_aborted"
    assert event["status"] == "FAILED"
    assert event["message"] == "Pipeline aborted: Load positions failed."
    assert "execution_id" in event
    assert "correlation_id" in event


def test_orchestration_emits_quality_report_metrics_on_failure(caplog):
    deps, recorder = FakeTransformLiveDataOrchestrationDependencies.create_scenario(
        load_raises=RuntimeError("storage unavailable")
    )
    caplog.clear()
    caplog.set_level("INFO")

    with pytest.raises(RuntimeError):
        load_transform_save_positions("transformlivedata", "2026-05-17T10:00:00+00:00", deps)

    events = _extract_quality_report_metrics_events(caplog)
    assert len(events) == 1
    event = events[0]
    assert event["status"] == "FAILED"
    assert event["metadata"]["failure_phase"] == "load_positions"
    assert "execution_phase_metrics" not in event.get("metadata", {})
    assert recorder.failure_quality_report_calls == 1
