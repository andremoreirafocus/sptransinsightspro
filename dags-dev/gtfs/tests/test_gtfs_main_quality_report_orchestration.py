import importlib.util
from pathlib import Path

import pytest
from gtfs.gtfs import StageExecutionError
from unittest.mock import patch


def load_gtfs_v5_module():
    module_path = Path(__file__).resolve().parents[2] / "gtfs-v5.py"
    spec = importlib.util.spec_from_file_location("gtfs_v5", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_main_calls_build_quality_report_on_success():
    """Test v5 orchestration: all tasks succeed, quality report is built."""
    calls = {"report": 0}

    def mock_extract_load_files(ctx, results):
        return {**results, "extract_load_files": {"status": "PASS", "validated_items_count": 1}}

    def mock_transform(ctx, results):
        return {**results, "transformation": {"status": "PASS", "validated_items_count": 6}}

    def mock_create_trip_details(ctx, results):
        return {**results, "enrichment": {"status": "PASS", "validated_items_count": 1}}

    def mock_build_report(ctx, results):
        calls["report"] += 1

    # Patch before loading module so imports see the mocked functions
    with patch("gtfs.gtfs.extract_load_files", mock_extract_load_files):
        with patch("gtfs.gtfs.transform", mock_transform):
            with patch("gtfs.gtfs.create_trip_details", mock_create_trip_details):
                with patch("gtfs.gtfs.build_quality_report_and_send_webhook", mock_build_report):
                    module = load_gtfs_v5_module()
                    module.main()

    assert calls["report"] == 1


def test_main_does_not_call_build_quality_report_on_stage_failure():
    """Test v5 orchestration: stage fails, quality report is not built in success path."""
    calls = {"report": 0}

    def mock_extract_load_files(ctx, results):
        return {**results, "extract_load_files": {"status": "PASS", "validated_items_count": 1}}

    def mock_transform_fails(ctx, results):
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

    # Patch before loading module so imports see the mocked functions
    with patch("gtfs.gtfs.extract_load_files", mock_extract_load_files):
        with patch("gtfs.gtfs.transform", mock_transform_fails):
            with patch("gtfs.gtfs.build_quality_report_and_send_webhook", mock_build_report):
                module = load_gtfs_v5_module()
                with pytest.raises(StageExecutionError):
                    module.main()

    assert calls["report"] == 0
