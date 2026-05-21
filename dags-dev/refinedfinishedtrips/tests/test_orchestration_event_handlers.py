import json
from datetime import datetime, timezone

import pytest

from observability.structured_event_logger import get_structured_logger
from quality.execution_phase_metrics import ExecutionPhaseMetricsTracker
from refinedfinishedtrips.domain.logger import RefinedFinishedTripsLogger
from refinedfinishedtrips.orchestration_dependencies import (
    RefinedFinishedTripsOrchestrationDependencies,
)
from refinedfinishedtrips.orchestration_event_handlers import (
    PipelineTaskRunState,
    handle_failure_event,
    handle_phase_metrics_event,
)

_RUN_TS = datetime(2026, 5, 17, 10, 0, 0, tzinfo=timezone.utc)

PHASE_ORDER = ["config_load", "positions_load", "positions_quality", "trip_extraction", "persistence", "quality_report"]


def _make_logger(name: str) -> RefinedFinishedTripsLogger:
    return RefinedFinishedTripsLogger(
        get_structured_logger(
            service="test_service",
            component="test_component",
            logger_name=name,
        )
    )


def _make_tracker() -> ExecutionPhaseMetricsTracker:
    return ExecutionPhaseMetricsTracker(
        pipeline="refinedfinishedtrips",
        execution_id="test-exec-id",
        logical_date_utc="2026-05-17T10:00:00+00:00",
        phase_order=PHASE_ORDER,
    )


def _make_state(**overrides) -> PipelineTaskRunState:
    defaults: dict = dict(
        execution_id="test-exec-id",
        correlation_id="test-exec-id",
        run_ts=_RUN_TS,
    )
    defaults.update(overrides)
    return PipelineTaskRunState(**defaults)


def _make_failure_deps(create_failure_quality_report=None) -> RefinedFinishedTripsOrchestrationDependencies:
    _noop = lambda *a, **k: None  # noqa: E731
    return RefinedFinishedTripsOrchestrationDependencies(
        get_config=_noop,
        get_recent_positions=_noop,
        get_all_finished_trips=_noop,
        validate_positions_quality=_noop,
        validate_trips_quality=_noop,
        validate_persistence_quality=_noop,
        save_finished_trips_to_db=_noop,
        create_quality_report=_noop,
        create_failure_quality_report=create_failure_quality_report or _noop,
        create_final_quality_report=_noop,
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
        correlation_id="exec-1",
        run_ts=_RUN_TS,
    )
    assert state.pipeline_config is None
    assert state.positions_result is None
    assert state.trips_result is None
    assert state.persistence_result is None
    assert state.column_lineage is None
    assert state.extraction_metrics == {}


def test_pipeline_task_run_state_is_mutable():
    state = PipelineTaskRunState(
        execution_id="exec-1",
        correlation_id="exec-1",
        run_ts=_RUN_TS,
    )
    state.pipeline_config = {"general": {}}
    state.positions_result = {"status": "PASS"}
    assert state.pipeline_config == {"general": {}}
    assert state.positions_result == {"status": "PASS"}


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
    assert event["metadata"]["pipeline"] == "refinedfinishedtrips"
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
    state = _make_state(correlation_id="corr-abc")
    tracker = _make_tracker()
    logger = _make_logger("test_pm_corr")

    handle_phase_metrics_event(state, tracker, logger, "success")

    events = _parse_log_events(caplog, "execution_phase_metrics")
    assert events[0]["correlation_id"] == "corr-abc"


def test_handle_phase_metrics_event_includes_all_phases(caplog):
    caplog.set_level("INFO")
    state = _make_state()
    tracker = _make_tracker()
    logger = _make_logger("test_pm_phases")

    handle_phase_metrics_event(state, tracker, logger, "success")

    events = _parse_log_events(caplog, "execution_phase_metrics")
    assert set(events[0]["metadata"]["phase_metrics"].keys()) == set(PHASE_ORDER)


# --- handle_failure_event ---


def test_handle_failure_event_skips_report_when_pipeline_config_is_none(caplog):
    caplog.set_level("ERROR")
    state = _make_state(pipeline_config=None)
    called = []
    deps = _make_failure_deps(
        create_failure_quality_report=lambda **k: called.append(k)
    )
    logger = _make_logger("test_fe_no_config")

    handle_failure_event(state, deps, logger, "trip_extraction", "failed")

    assert called == []
    events = _parse_log_events(caplog, "failure_report_skipped")
    assert len(events) == 1
    assert events[0]["status"] == "FAILED"


def test_handle_failure_event_calls_create_failure_quality_report_with_state(caplog):
    caplog.set_level("INFO")
    state = _make_state(
        pipeline_config={"general": {}},
        positions_result={"status": "PASS"},
        trips_result={"status": "PASS"},
        persistence_result=None,
        column_lineage={"table_name": "finished_trips"},
    )
    received: dict = {}

    def capture(**kwargs):
        received.update(kwargs)

    deps = _make_failure_deps(create_failure_quality_report=capture)
    logger = _make_logger("test_fe_call")

    handle_failure_event(state, deps, logger, "persistence", "save failed")

    assert received["failure_phase"] == "persistence"
    assert received["failure_message"] == "save failed"
    assert received["positions_result"] == {"status": "PASS"}
    assert received["trips_result"] == {"status": "PASS"}
    assert received["persistence_result"] is None
    assert received["column_lineage"] == {"table_name": "finished_trips"}
    assert received["execution_id"] == "test-exec-id"
    assert received["run_ts"] == _RUN_TS


def test_handle_failure_event_emits_failure_report_error_when_deps_raises(caplog):
    caplog.set_level("ERROR")
    state = _make_state(pipeline_config={"general": {}})

    def failing_report(**kwargs):
        raise RuntimeError("storage unavailable")

    deps = _make_failure_deps(create_failure_quality_report=failing_report)
    logger = _make_logger("test_fe_raises")

    handle_failure_event(state, deps, logger, "trip_extraction", "exploded")

    events = _parse_log_events(caplog, "failure_report_error")
    assert len(events) == 1
    event = events[0]
    assert event["level"] == "ERROR"
    assert event["error_type"] == "RuntimeError"
    assert event["error_message"] == "storage unavailable"
    assert event["execution_id"] == "test-exec-id"


def test_handle_failure_event_swallows_exception_from_deps(caplog):
    caplog.set_level("ERROR")
    state = _make_state(pipeline_config={"general": {}})

    def failing_report(**kwargs):
        raise ValueError("boom")

    deps = _make_failure_deps(create_failure_quality_report=failing_report)
    logger = _make_logger("test_fe_swallow")

    handle_failure_event(state, deps, logger, "persistence", "msg")
