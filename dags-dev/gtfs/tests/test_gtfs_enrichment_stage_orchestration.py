from io import BytesIO

import pandas as pd
import pytest
from gtfs.gtfs import create_trip_details, StageExecutionError
from unittest.mock import patch


def make_pipeline_config():
    return {
        "general": {
            "storage": {
                "trusted_bucket": "trusted",
            },
            "tables": {
                "trip_details_table_name": "trip_details",
            },
            "notifications": {"webhook_url": "disabled"},
        },
        "connections": {
            "object_storage": {
                "endpoint": "localhost",
                "access_key": "key",
                "secret_key": "secret",
            }
        },
        "data_expectations_trip_details": {
            "expectation_suite_name": "gtfs_trip_details",
            "expectations": [{"expectation_type": "expect_column_values_to_not_be_null"}],
        },
    }


def make_run_context():
    return {"execution_id": "exec-test", "batch_ts": "2026-04-19T10:00:00+00:00"}


def build_trip_details_parquet_buffer():
    df = pd.DataFrame(
        {
            "trip_id": ["t1"],
            "first_stop_id": [1],
            "first_stop_name": ["A"],
            "first_stop_lat": [-23.5],
            "first_stop_lon": [-46.6],
            "last_stop_id": [2],
            "last_stop_name": ["B"],
            "last_stop_lat": [-23.4],
            "last_stop_lon": [-46.5],
            "trip_linear_distance": [1000],
            "is_circular": [False],
        }
    )
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer


def build_trip_details_parquet_buffer_with_drift():
    df = pd.DataFrame(
        {
            "trip_id": ["t1"],
            "first_stop_id": [1],
            "first_stop_name": ["A"],
            "first_stop_lat": [-23.5],
            "first_stop_lon": [-46.6],
            "last_stop_id": [2],
            "last_stop_name": ["B"],
            "last_stop_lat": [-23.4],
            "last_stop_lon": [-46.5],
            "trip_linear_distance": [1000],
            "is_circular": [False],
            "new_field": ["x"],
        }
    )
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer


def test_create_trip_details_moves_to_final_on_success():
    config = make_pipeline_config()
    relocate_calls = []

    with patch("gtfs.gtfs.load_pipeline_config", return_value=config):
        with patch(
            "gtfs.gtfs.create_trip_details_table_and_fill_missing_data",
            return_value={
                "table_name": "trip_details",
                "staging_object_name": "gtfs/staging/trip_details.parquet",
                "row_count": 1,
                "staged_written": True,
            },
        ):
            with patch(
                "gtfs.gtfs.read_file_from_object_storage_to_bytesio",
                return_value=build_trip_details_parquet_buffer(),
            ):
                with patch(
                    "gtfs.gtfs.validate_expectations",
                    return_value={
                        "valid_df": pd.DataFrame(),
                        "invalid_df": None,
                        "expectations_summary": {
                            "rows_failed": 0,
                            "expectations_with_violations": 0,
                            "expectations_failed_due_to_exceptions": 0,
                        },
                    },
                ):
                    with patch(
                        "gtfs.gtfs.relocate_staged_trusted_files",
                        side_effect=lambda cfg, sr, target: (
                            relocate_calls.append(target),
                            {"status": "SUCCESS", "moved": [], "errors": []},
                        )[1],
                    ):
                        result = create_trip_details(make_run_context(), {})

    stage = result["enrichment"]
    assert stage["status"] == "PASS"
    assert stage["relocation_status"] == "SUCCESS"
    assert stage["artifacts"]["column_lineage"]["warning"] is None
    assert "relocation_details" in stage
    assert stage["relocation_details"]["errors"] == []
    assert relocate_calls == ["final"]


def test_create_trip_details_sets_lineage_warning_on_drift():
    config = make_pipeline_config()
    relocate_calls = []

    with patch("gtfs.gtfs.load_pipeline_config", return_value=config):
        with patch(
            "gtfs.gtfs.create_trip_details_table_and_fill_missing_data",
            return_value={
                "table_name": "trip_details",
                "staging_object_name": "gtfs/staging/trip_details.parquet",
                "row_count": 1,
                "staged_written": True,
            },
        ):
            with patch(
                "gtfs.gtfs.read_file_from_object_storage_to_bytesio",
                return_value=build_trip_details_parquet_buffer_with_drift(),
            ):
                with patch(
                    "gtfs.gtfs.validate_expectations",
                    return_value={
                        "valid_df": pd.DataFrame(),
                        "invalid_df": None,
                        "expectations_summary": {
                            "rows_failed": 0,
                            "expectations_with_violations": 0,
                            "expectations_failed_due_to_exceptions": 0,
                        },
                    },
                ):
                    with patch(
                        "gtfs.gtfs.relocate_staged_trusted_files",
                        side_effect=lambda cfg, sr, target: (
                            relocate_calls.append(target),
                            {"status": "SUCCESS", "moved": [], "errors": []},
                        )[1],
                    ):
                        result = create_trip_details(make_run_context(), {})

    stage = result["enrichment"]
    assert stage["status"] == "PASS"
    assert stage["artifacts"]["column_lineage"]["warning"] == "lineage drift detected"
    assert "relocation_details" in stage
    assert stage["relocation_details"]["errors"] == []
    assert relocate_calls == ["final"]


def test_create_trip_details_quarantines_on_gx_failure():
    config = make_pipeline_config()
    relocate_calls = []

    with patch("gtfs.gtfs.load_pipeline_config", return_value=config):
        with patch(
            "gtfs.gtfs.create_trip_details_table_and_fill_missing_data",
            return_value={
                "table_name": "trip_details",
                "staging_object_name": "gtfs/staging/trip_details.parquet",
                "row_count": 1,
                "staged_written": True,
            },
        ):
            with patch(
                "gtfs.gtfs.read_file_from_object_storage_to_bytesio",
                return_value=build_trip_details_parquet_buffer(),
            ):
                with patch(
                    "gtfs.gtfs.validate_expectations",
                    return_value={
                        "valid_df": pd.DataFrame(),
                        "invalid_df": pd.DataFrame({"col": [1]}),
                        "expectations_summary": {
                            "rows_failed": 1,
                            "expectations_with_violations": 1,
                            "expectations_failed_due_to_exceptions": 0,
                        },
                    },
                ):
                    with patch(
                        "gtfs.gtfs.relocate_staged_trusted_files",
                        side_effect=lambda cfg, sr, target: (
                            relocate_calls.append(target),
                            {"status": "SUCCESS", "moved": [], "errors": []},
                        )[1],
                    ):
                        with patch("gtfs.gtfs.handle_unexpected_error"):
                            with pytest.raises(StageExecutionError) as excinfo:
                                create_trip_details(make_run_context(), {})

    stage_result = excinfo.value.stage_result
    assert "relocation_details" in stage_result
    assert stage_result["relocation_details"]["errors"] == []
    assert relocate_calls == ["quarantine"]


def test_create_trip_details_quarantines_when_validation_raises_after_staging():
    config = make_pipeline_config()
    relocate_calls = []

    with patch("gtfs.gtfs.load_pipeline_config", return_value=config):
        with patch(
            "gtfs.gtfs.create_trip_details_table_and_fill_missing_data",
            return_value={
                "table_name": "trip_details",
                "staging_object_name": "gtfs/staging/trip_details.parquet",
                "row_count": 1,
                "staged_written": True,
            },
        ):
            with patch(
                "gtfs.gtfs.read_file_from_object_storage_to_bytesio",
                side_effect=RuntimeError("read failed"),
            ):
                with patch(
                    "gtfs.gtfs.relocate_staged_trusted_files",
                    side_effect=lambda cfg, sr, target: (
                        relocate_calls.append(target),
                        {"status": "SUCCESS", "moved": [], "errors": []},
                    )[1],
                ):
                    with patch("gtfs.gtfs.handle_unexpected_error"):
                        with pytest.raises(StageExecutionError) as excinfo:
                            create_trip_details(make_run_context(), {})

    stage_result = excinfo.value.stage_result
    assert "relocation_details" in stage_result
    assert stage_result["relocation_details"]["errors"] == []
    assert relocate_calls == ["quarantine"]
