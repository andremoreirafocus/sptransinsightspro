import pytest
from gtfs.gtfs import extract_load_files, StageExecutionError
from gtfs.tests.fakes.fake_gtfs_orchestration_dependencies import (
    FakeGtfsOrchestrationDependencies,
)


def make_run_context():
    return {"execution_id": "exec-test", "batch_ts": "2026-04-19T10:00:00+00:00"}


def test_extract_load_files_quarantines_and_raises_on_validation_failure():
    deps, recorder = FakeGtfsOrchestrationDependencies.create_scenario(
        extracted_files=["stops.txt", "routes.txt"],
        validation_result={
            "is_valid": False,
            "errors_by_file": {"stops.txt": ["insufficient_lines"]},
            "validated_files_count": 2,
        },
    )

    with pytest.raises(StageExecutionError, match="Raw GTFS validation failed"):
        extract_load_files(make_run_context(), {}, deps)

    assert len(recorder.save_raw_calls) == 1
    assert recorder.save_raw_calls[0]["failed"] is True


def test_extract_load_files_saves_raw_on_success():
    deps, recorder = FakeGtfsOrchestrationDependencies.create_scenario(
        extracted_files=["stops.txt"],
        validation_result={
            "is_valid": True,
            "errors_by_file": {},
            "validated_files_count": 1,
        },
    )

    result = extract_load_files(make_run_context(), {}, deps)

    assert len(recorder.save_raw_calls) == 1
    assert recorder.save_raw_calls[0]["failed"] is False
    assert result["extract_load_files"]["status"] == "PASS"
    assert result["extract_load_files"]["validated_items_count"] == 1


def test_extract_load_files_raises_stage_execution_error_when_no_files_extracted():
    deps, _ = FakeGtfsOrchestrationDependencies.create_scenario(
        extracted_files=[],
    )

    with pytest.raises(StageExecutionError, match="No GTFS files extracted"):
        extract_load_files(make_run_context(), {}, deps)


def test_extract_load_files_raises_stage_execution_error_on_extract_failure():
    deps, _ = FakeGtfsOrchestrationDependencies.create_scenario(
        extract_raises=RuntimeError("download failed"),
    )

    with pytest.raises(StageExecutionError, match="download failed"):
        extract_load_files(make_run_context(), {}, deps)
