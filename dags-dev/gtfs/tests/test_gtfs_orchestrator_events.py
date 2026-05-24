import json

import pytest

from observability.structured_event_logger import clear_execution_context
from gtfs.gtfs import (
    build_quality_report_and_send_webhook,
    build_run_context,
    create_trip_details,
    extract_load_files,
    transform,
    StageExecutionError,
)
from gtfs.tests.fakes.fake_gtfs_orchestration_dependencies import (
    FakeGtfsOrchestrationDependencies,
)


@pytest.fixture(autouse=True)
def _clear_execution_context():
    clear_execution_context()
    yield
    clear_execution_context()


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


def _run_full_pipeline(deps):
    run_context = build_run_context()
    stage_results = {}
    stage_results = extract_load_files(run_context, stage_results, deps)
    stage_results = transform(run_context, stage_results, deps)
    stage_results = create_trip_details(run_context, stage_results, deps)
    build_quality_report_and_send_webhook(run_context, stage_results, deps)
    return run_context, stage_results


def test_success_emits_execution_started(caplog):
    caplog.set_level("INFO")
    deps, _ = FakeGtfsOrchestrationDependencies.create_scenario()

    _run_full_pipeline(deps)

    events = _parse_log_events(caplog, "execution_started")
    assert len(events) == 1
    assert events[0]["status"] == "STARTED"


def test_success_emits_execution_finished(caplog):
    caplog.set_level("INFO")
    deps, _ = FakeGtfsOrchestrationDependencies.create_scenario()

    _run_full_pipeline(deps)

    events = _parse_log_events(caplog, "execution_finished")
    assert len(events) == 1
    assert events[0]["status"] == "SUCCEEDED"


def test_success_emits_execution_phase_metrics_with_all_phases(caplog):
    caplog.set_level("INFO")
    deps, _ = FakeGtfsOrchestrationDependencies.create_scenario()

    _run_full_pipeline(deps)

    events = _parse_log_events(caplog, "execution_phase_metrics")
    assert len(events) >= 1
    # The final emission (from build_quality_report) has all 4 phases
    final = events[-1]
    phase_metrics = final["metadata"]["phase_metrics"]
    assert set(phase_metrics.keys()) == {
        "extract_load_files",
        "transformation",
        "enrichment",
        "quality_report",
    }
    assert phase_metrics["extract_load_files"]["status"] == "success"
    assert phase_metrics["transformation"]["status"] == "success"
    assert phase_metrics["enrichment"]["status"] == "success"
    assert phase_metrics["quality_report"]["status"] == "success"


def test_extract_load_failure_emits_execution_aborted(caplog):
    caplog.set_level("ERROR")
    deps, _ = FakeGtfsOrchestrationDependencies.create_scenario(
        extract_raises=RuntimeError("download failed"),
    )

    with pytest.raises(StageExecutionError):
        run_context = build_run_context()
        extract_load_files(run_context, {}, deps)

    events = _parse_log_events(caplog, "execution_aborted")
    assert len(events) >= 1
    assert any(e["metadata"]["phase"] == "extract_load_files" for e in events)


def test_transform_failure_emits_execution_aborted(caplog):
    caplog.set_level("ERROR")
    deps, _ = FakeGtfsOrchestrationDependencies.create_scenario(
        transform_raises=ValueError("transform boom"),
    )

    with pytest.raises(StageExecutionError):
        run_context = build_run_context()
        stage_results = {}
        stage_results = extract_load_files(run_context, stage_results, deps)
        transform(run_context, stage_results, deps)

    events = _parse_log_events(caplog, "execution_aborted")
    assert len(events) >= 1
    assert any(e["metadata"]["phase"] == "transformation" for e in events)


def test_enrichment_failure_emits_execution_aborted(caplog):
    caplog.set_level("ERROR")
    deps, _ = FakeGtfsOrchestrationDependencies.create_scenario(
        create_trip_details_raises=ValueError("enrichment boom"),
    )

    with pytest.raises(StageExecutionError):
        run_context = build_run_context()
        stage_results = {}
        stage_results = extract_load_files(run_context, stage_results, deps)
        stage_results = transform(run_context, stage_results, deps)
        create_trip_details(run_context, stage_results, deps)

    events = _parse_log_events(caplog, "execution_aborted")
    assert len(events) >= 1
    assert any(e["metadata"]["phase"] == "enrichment" for e in events)


def test_quality_report_failure_emits_execution_aborted(caplog):
    caplog.set_level("ERROR")
    deps, _ = FakeGtfsOrchestrationDependencies.create_scenario(
        create_report_raises=RuntimeError("report boom"),
    )

    with pytest.raises(RuntimeError):
        run_context = build_run_context()
        stage_results = {}
        stage_results = extract_load_files(run_context, stage_results, deps)
        stage_results = transform(run_context, stage_results, deps)
        stage_results = create_trip_details(run_context, stage_results, deps)
        build_quality_report_and_send_webhook(run_context, stage_results, deps)

    events = _parse_log_events(caplog, "execution_aborted")
    assert len(events) >= 1
    assert any(e["metadata"]["phase"] == "quality_report" for e in events)


def test_failure_phase_metrics_includes_completed_phases(caplog):
    caplog.set_level("ERROR")
    deps, _ = FakeGtfsOrchestrationDependencies.create_scenario(
        transform_raises=ValueError("transform boom"),
    )

    with pytest.raises(StageExecutionError):
        run_context = build_run_context()
        stage_results = {}
        stage_results = extract_load_files(run_context, stage_results, deps)
        transform(run_context, stage_results, deps)

    events = _parse_log_events(caplog, "execution_phase_metrics")
    assert len(events) >= 1
    # Find the transformation failure metrics
    transform_failure = next(
        e for e in events
        if e["metadata"].get("overall_status") == "failed"
        and e["metadata"]["phase_metrics"].get("transformation", {}).get("status") == "failed"
    )
    phase_metrics = transform_failure["metadata"]["phase_metrics"]
    assert phase_metrics["extract_load_files"]["status"] == "success"
    assert phase_metrics["transformation"]["status"] == "failed"
