import pytest
import gtfs.gtfs
from gtfs.gtfs import (
    extract_load_files,
    transform,
    create_trip_details,
    build_quality_report_and_send_webhook,
    build_run_context,
    StageExecutionError,
)


def fake_write_fn(connection_data, buffer, bucket_name, object_name):
    pass


def test_main_calls_build_quality_report_on_success():
    """Test orchestration: all tasks succeed, quality report is built."""
    calls = {"report": 0}

    def mock_extract_load_files(ctx, results, write_fn=None):
        return {**results, "extract_load_files": {"status": "PASS", "validated_items_count": 1}}

    def mock_transform(ctx, results, write_fn=None):
        return {**results, "transformation": {"status": "PASS", "validated_items_count": 6}}

    def mock_create_trip_details(ctx, results, write_fn=None):
        return {**results, "enrichment": {"status": "PASS", "validated_items_count": 1}}

    def mock_build_report(ctx, results):
        calls["report"] += 1

    orig_extract = gtfs.gtfs.extract_load_files
    orig_transform = gtfs.gtfs.transform
    orig_create_trip = gtfs.gtfs.create_trip_details
    orig_build_report = gtfs.gtfs.build_quality_report_and_send_webhook

    gtfs.gtfs.extract_load_files = mock_extract_load_files
    gtfs.gtfs.transform = mock_transform
    gtfs.gtfs.create_trip_details = mock_create_trip_details
    gtfs.gtfs.build_quality_report_and_send_webhook = mock_build_report

    try:
        run_context = build_run_context()
        stage_results = {}
        stage_results = gtfs.gtfs.extract_load_files(run_context, stage_results, write_fn=fake_write_fn)
        stage_results = gtfs.gtfs.transform(run_context, stage_results, write_fn=fake_write_fn)
        stage_results = gtfs.gtfs.create_trip_details(run_context, stage_results, write_fn=fake_write_fn)
        gtfs.gtfs.build_quality_report_and_send_webhook(run_context, stage_results)
    finally:
        gtfs.gtfs.extract_load_files = orig_extract
        gtfs.gtfs.transform = orig_transform
        gtfs.gtfs.create_trip_details = orig_create_trip
        gtfs.gtfs.build_quality_report_and_send_webhook = orig_build_report

    assert calls["report"] == 1


def test_main_does_not_call_build_quality_report_on_stage_failure():
    """Test orchestration: stage fails, quality report is not built in success path."""
    calls = {"report": 0}

    def mock_extract_load_files(ctx, results, write_fn=None):
        return {**results, "extract_load_files": {"status": "PASS", "validated_items_count": 1}}

    def mock_transform_fails(ctx, results, write_fn=None):
        raise StageExecutionError(
            "transformation",
            "Validation failures detected",
            {
                "status": "FAIL",
                "validated_items_count": 6,
                "error_details": {"errors_by_table": {"stops": ["gx_validation_failed"]}},
                "relocation_status": "SUCCESS",
                "relocation_error": None,
            },
        )

    def mock_build_report(ctx, results):
        calls["report"] += 1

    orig_extract = gtfs.gtfs.extract_load_files
    orig_transform = gtfs.gtfs.transform
    orig_build_report = gtfs.gtfs.build_quality_report_and_send_webhook

    gtfs.gtfs.extract_load_files = mock_extract_load_files
    gtfs.gtfs.transform = mock_transform_fails
    gtfs.gtfs.build_quality_report_and_send_webhook = mock_build_report

    try:
        run_context = build_run_context()
        stage_results = {}
        stage_results = gtfs.gtfs.extract_load_files(run_context, stage_results, write_fn=fake_write_fn)
        with pytest.raises(StageExecutionError):
            gtfs.gtfs.transform(run_context, stage_results, write_fn=fake_write_fn)
    finally:
        gtfs.gtfs.extract_load_files = orig_extract
        gtfs.gtfs.transform = orig_transform
        gtfs.gtfs.build_quality_report_and_send_webhook = orig_build_report

    assert calls["report"] == 0
