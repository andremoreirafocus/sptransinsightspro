import pytest
import gtfs.gtfs
from gtfs.gtfs import extract_load_files, StageExecutionError
from gtfs.tests.fakes.fake_orchestration_dependencies import (
    FakeExtractLoadDependencies,
)


def make_pipeline_config():
    return {
        "general": {
            "notifications": {"webhook_url": "disabled"},
            "storage": {
                "metadata_bucket": "meta-bucket",
                "quality_report_folder": "quality",
            },
        },
        "connections": {
            "object_storage": {
                "endpoint": "localhost",
                "access_key": "key",
                "secret_key": "secret",
            }
        },
    }


def make_run_context():
    return {"execution_id": "exec-test", "batch_ts": "2026-04-19T10:00:00+00:00"}


def fake_write_fn(connection_data, buffer, bucket_name, object_name):
    pass


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

    orig_load_config = gtfs.gtfs.load_pipeline_config
    orig_extract = gtfs.gtfs.extract_gtfs_files
    orig_validate = gtfs.gtfs.validate_raw_gtfs_files
    orig_save = gtfs.gtfs.save_files_to_raw_storage
    orig_handle_error = gtfs.gtfs.handle_unexpected_error

    gtfs.gtfs.load_pipeline_config = deps.load_pipeline_config
    gtfs.gtfs.extract_gtfs_files = deps.extract_gtfs_files
    gtfs.gtfs.validate_raw_gtfs_files = deps.validate_raw_gtfs_files
    gtfs.gtfs.save_files_to_raw_storage = deps.save_files_to_raw_storage
    gtfs.gtfs.handle_unexpected_error = lambda e, run_context, stage_results, write_fn=None: None

    try:
        with pytest.raises(StageExecutionError, match="Raw GTFS validation failed"):
            extract_load_files(make_run_context(), {}, write_fn=fake_write_fn)
    finally:
        gtfs.gtfs.load_pipeline_config = orig_load_config
        gtfs.gtfs.extract_gtfs_files = orig_extract
        gtfs.gtfs.validate_raw_gtfs_files = orig_validate
        gtfs.gtfs.save_files_to_raw_storage = orig_save
        gtfs.gtfs.handle_unexpected_error = orig_handle_error

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

    orig_load_config = gtfs.gtfs.load_pipeline_config
    orig_extract = gtfs.gtfs.extract_gtfs_files
    orig_validate = gtfs.gtfs.validate_raw_gtfs_files
    orig_save = gtfs.gtfs.save_files_to_raw_storage

    gtfs.gtfs.load_pipeline_config = deps.load_pipeline_config
    gtfs.gtfs.extract_gtfs_files = deps.extract_gtfs_files
    gtfs.gtfs.validate_raw_gtfs_files = deps.validate_raw_gtfs_files
    gtfs.gtfs.save_files_to_raw_storage = deps.save_files_to_raw_storage

    try:
        result = extract_load_files(make_run_context(), {}, write_fn=fake_write_fn)
    finally:
        gtfs.gtfs.load_pipeline_config = orig_load_config
        gtfs.gtfs.extract_gtfs_files = orig_extract
        gtfs.gtfs.validate_raw_gtfs_files = orig_validate
        gtfs.gtfs.save_files_to_raw_storage = orig_save

    assert len(deps.raw_save_calls) == 1
    assert deps.raw_save_calls[0]["failed"] is False
    assert result["extract_load_files"]["status"] == "PASS"
    assert result["extract_load_files"]["validated_items_count"] == 1
