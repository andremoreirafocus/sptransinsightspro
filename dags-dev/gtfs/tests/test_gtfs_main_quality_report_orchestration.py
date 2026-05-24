import pytest
from gtfs.gtfs import (
    build_run_context,
    build_quality_report,
    extract_load_files,
    transform,
    create_trip_details,
    StageExecutionError,
)
from gtfs.tests.fakes.fake_gtfs_orchestration_dependencies import (
    FakeGtfsOrchestrationDependencies,
)


def test_main_calls_build_quality_report_on_success():
    """Test orchestration: all tasks succeed, quality report is built."""
    deps, recorder = FakeGtfsOrchestrationDependencies.create_scenario()

    run_context = build_run_context()
    stage_results = {}
    stage_results = extract_load_files(run_context, stage_results, deps)
    stage_results = transform(run_context, stage_results, deps)
    stage_results = create_trip_details(run_context, stage_results, deps)
    build_quality_report(run_context, stage_results, deps)

    assert len(recorder.create_report_calls) == 1


def test_main_does_not_call_build_quality_report_on_stage_failure():
    """Test orchestration: stage fails, quality report is not built in success path."""
    deps, recorder = FakeGtfsOrchestrationDependencies.create_scenario(
        transform_raises=ValueError("Validation failures detected"),
    )

    run_context = build_run_context()
    stage_results = {}
    stage_results = extract_load_files(run_context, stage_results, deps)
    with pytest.raises(StageExecutionError):
        transform(run_context, stage_results, deps)

    assert len(recorder.create_report_calls) == 0
