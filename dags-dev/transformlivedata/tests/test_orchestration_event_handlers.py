import json

import pandas as pd

from observability.structured_event_logger import get_structured_logger
from quality.execution_phase_metrics import ExecutionPhaseMetricsTracker
from transformlivedata.domain.logger import TransformLivedataLogger
from transformlivedata.orchestration_dependencies import (
    TransformLiveDataOrchestrationDependencies,
)
from transformlivedata.orchestration_event_handlers import (
    PipelineTaskRunState,
    handle_failure_event,
    handle_phase_metrics_event,
)


def _make_logger(name: str) -> TransformLivedataLogger:
    return TransformLivedataLogger(
        get_structured_logger(
            service="test_service",
            component="test_component",
            logger_name=name,
        )
    )


def _make_tracker(pipeline: str = "test_pipeline") -> ExecutionPhaseMetricsTracker:
    return ExecutionPhaseMetricsTracker(
        pipeline=pipeline,
        execution_id="test-exec-id",
        logical_date_utc="2026-05-17T10:00:00+00:00",
        phase_order=["phase_a", "phase_b"],
    )


def _make_state(**overrides) -> PipelineTaskRunState:
    defaults: dict = dict(
        execution_id="test-exec-id",
        correlation_id="2026-05-17T10:00:00+00:00",
        logical_date_utc="2026-05-17T10:00:00+00:00",
    )
    defaults.update(overrides)
    return PipelineTaskRunState(**defaults)


def _make_failure_deps(
    create_failure_quality_report=None,
) -> TransformLiveDataOrchestrationDependencies:
    _noop = lambda *a, **k: None  # noqa: E731
    return TransformLiveDataOrchestrationDependencies(
        build_logical_date_context=_noop,
        get_config=_noop,
        load_positions=_noop,
        validate_json_data_schema=_noop,
        transform_positions=_noop,
        validate_expectations=_noop,
        save_positions_to_storage=_noop,
        mark_request_as_processed=_noop,
        create_data_quality_report=_noop,
        create_failure_quality_report=create_failure_quality_report or _noop,
    )


def _parse_log_events(caplog, event_name: str) -> list[dict]:
    results = []
    for record in caplog.records:
        try:
            parsed = json.loads(record.getMessage())
        except Exception:
            continue
        if parsed.get("event") == event_name:
            results.append(parsed)
    return results


# --- PipelineTaskRunState ---


def test_pipeline_task_run_state_has_correct_defaults():
    state = PipelineTaskRunState(
        execution_id="exec-1",
        correlation_id="2026-05-17",
        logical_date_utc="2026-05-17",
    )
    assert state.pipeline_config is None
    assert state.source_file == ""
    assert state.transform_result is None
    assert state.expectations_result is None
    assert state.quarantine_save_status == "SKIPPED"
    assert state.quarantine_save_error is None


# --- handle_phase_metrics_event ---


def test_handle_phase_metrics_event_success_emits_info_with_succeeded_status(caplog):
    caplog.set_level("INFO")
    state = _make_state()
    tracker = _make_tracker()
    logger = _make_logger("test_pm_success")

    handle_phase_metrics_event(state, tracker, logger, "success")

    events = _parse_log_events(caplog, "execution_phase_metrics")
    assert len(events) == 1
    event = events[0]
    assert event["level"] == "INFO"
    assert event["status"] == "SUCCEEDED"
    assert event["metadata"]["overall_status"] == "success"
    assert event["metadata"]["pipeline"] == "test_pipeline"
    assert event["execution_id"] == "test-exec-id"


def test_handle_phase_metrics_event_failure_emits_error_with_failed_status(caplog):
    caplog.set_level("ERROR")
    state = _make_state()
    tracker = _make_tracker()
    logger = _make_logger("test_pm_failure")

    handle_phase_metrics_event(state, tracker, logger, "failed")

    events = _parse_log_events(caplog, "execution_phase_metrics")
    assert len(events) == 1
    event = events[0]
    assert event["level"] == "ERROR"
    assert event["status"] == "FAILED"
    assert event["metadata"]["overall_status"] == "failed"


def test_handle_phase_metrics_event_includes_correlation_id(caplog):
    caplog.set_level("INFO")
    state = _make_state(correlation_id="corr-123")
    tracker = _make_tracker()
    logger = _make_logger("test_pm_corr_id")

    handle_phase_metrics_event(state, tracker, logger, "success")

    events = _parse_log_events(caplog, "execution_phase_metrics")
    assert events[0]["correlation_id"] == "corr-123"


# --- handle_failure_event ---


def test_handle_failure_event_skips_report_when_pipeline_config_is_none(caplog):
    caplog.set_level("ERROR")
    state = _make_state(pipeline_config=None)
    deps = _make_failure_deps()
    logger = _make_logger("test_fe_no_config")

    handle_failure_event(state, deps, logger, "load_positions", "Load failed")

    events = _parse_log_events(caplog, "failure_report_skipped")
    assert len(events) == 1
    assert events[0]["level"] == "ERROR"
    assert events[0]["status"] == "FAILED"


def test_handle_failure_event_emits_quality_report_metrics_with_failure_metadata(caplog):
    caplog.set_level("INFO")
    state = _make_state(pipeline_config={"general": {}}, source_file="posicoes.json")

    def fake_report(**kwargs):
        return {
            "summary_status": "FAIL",
            "failure_phase": kwargs["failure_phase"],
            "failure_message": kwargs["failure_message"],
        }

    deps = _make_failure_deps(create_failure_quality_report=fake_report)
    logger = _make_logger("test_fe_metrics")

    handle_failure_event(state, deps, logger, "transform", "Transformation failed")

    events = _parse_log_events(caplog, "quality_report_metrics")
    assert len(events) == 1
    event = events[0]
    assert event["status"] == "FAILED"
    assert event["metadata"]["failure_phase"] == "transform"
    assert event["metadata"]["failure_message"] == "Transformation failed"


def test_handle_failure_event_emits_failure_report_error_when_deps_raises(caplog):
    caplog.set_level("ERROR")
    state = _make_state(pipeline_config={"general": {}})

    def failing_report(**kwargs):
        raise RuntimeError("report storage unavailable")

    deps = _make_failure_deps(create_failure_quality_report=failing_report)
    logger = _make_logger("test_fe_deps_raises")

    handle_failure_event(state, deps, logger, "transform", "Transformation failed")

    events = _parse_log_events(caplog, "failure_report_error")
    assert len(events) == 1
    event = events[0]
    assert event["level"] == "ERROR"
    assert event["error_type"] == "RuntimeError"
    assert event["error_message"] == "report storage unavailable"


def test_handle_failure_event_passes_batch_ts_from_transform_result():
    transform_result = {
        "positions": pd.DataFrame([{"col": 1}]),
        "invalid_positions": pd.DataFrame(),
        "batch_ts": "2026-05-17T10:05:00+00:00",
    }
    state = _make_state(
        pipeline_config={"general": {}},
        transform_result=transform_result,
    )

    received: dict = {}

    def capture_report(**kwargs):
        received["batch_ts"] = kwargs.get("batch_ts")
        return {"summary_status": "FAIL"}

    deps = _make_failure_deps(create_failure_quality_report=capture_report)
    logger = _make_logger("test_fe_batch_ts")

    handle_failure_event(state, deps, logger, "save_trusted", "Save failed")

    assert received["batch_ts"] == "2026-05-17T10:05:00+00:00"


def test_handle_failure_event_uses_logical_date_as_batch_ts_when_no_transform_result():
    state = _make_state(
        pipeline_config={"general": {}},
        transform_result=None,
        logical_date_utc="2026-05-17T10:00:00+00:00",
    )

    received: dict = {}

    def capture_report(**kwargs):
        received["batch_ts"] = kwargs.get("batch_ts")
        return {"summary_status": "FAIL"}

    deps = _make_failure_deps(create_failure_quality_report=capture_report)
    logger = _make_logger("test_fe_batch_ts_fallback")

    handle_failure_event(state, deps, logger, "load_positions", "Load failed")

    assert received["batch_ts"] == "2026-05-17T10:00:00+00:00"
