import pytest
from gtfs.gtfs import extract_load_files, StageExecutionError
from gtfs.tests.fakes.fake_orchestration_dependencies import (
    FakeExtractLoadDependencies,
)
from unittest.mock import patch


def make_pipeline_config():
    return {
        "general": {
            "notifications": {"webhook_url": "disabled"},
        }
    }


def make_run_context():
    return {"execution_id": "exec-test", "batch_ts": "2026-04-19T10:00:00+00:00"}


def test_extract_load_files_quarantines_and_raises_on_validation_failure():
    deps = FakeExtractLoadDependencies(
        pipeline_config=make_pipeline_config(),
        extracted_files=["stops.txt", "routes.txt"],
        validation_result={
            "is_valid": False,
            "errors_by_file": {"stops.txt": ["insufficient_lines"]},
            "validated_files_count": 2,
        },
    )

    with patch("gtfs.gtfs.load_pipeline_config", deps.load_pipeline_config):
        with patch("gtfs.gtfs.extract_gtfs_files", deps.extract_gtfs_files):
            with patch("gtfs.gtfs.validate_raw_gtfs_files", deps.validate_raw_gtfs_files):
                with patch("gtfs.gtfs.save_files_to_raw_storage", deps.save_files_to_raw_storage):
                    with patch("gtfs.gtfs.handle_unexpected_error"):
                        with pytest.raises(StageExecutionError, match="Raw GTFS validation failed"):
                            extract_load_files(make_run_context(), {})

    assert len(deps.raw_save_calls) == 1
    assert deps.raw_save_calls[0]["failed"] is True


def test_extract_load_files_saves_raw_on_success():
    deps = FakeExtractLoadDependencies(
        pipeline_config=make_pipeline_config(),
        extracted_files=["stops.txt"],
        validation_result={
            "is_valid": True,
            "errors_by_file": {},
            "validated_files_count": 1,
        },
    )

    with patch("gtfs.gtfs.load_pipeline_config", deps.load_pipeline_config):
        with patch("gtfs.gtfs.extract_gtfs_files", deps.extract_gtfs_files):
            with patch("gtfs.gtfs.validate_raw_gtfs_files", deps.validate_raw_gtfs_files):
                with patch("gtfs.gtfs.save_files_to_raw_storage", deps.save_files_to_raw_storage):
                    result = extract_load_files(make_run_context(), {})

    assert len(deps.raw_save_calls) == 1
    assert deps.raw_save_calls[0]["failed"] is False
    assert result["extract_load_files"]["status"] == "PASS"
    assert result["extract_load_files"]["validated_items_count"] == 1
