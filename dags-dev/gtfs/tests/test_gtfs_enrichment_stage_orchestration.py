from io import BytesIO

import pandas as pd
import pytest
import gtfs.gtfs
from gtfs.gtfs import create_trip_details, StageExecutionError
from gtfs.tests.fakes.fake_orchestration_dependencies import (
    FakeTripDetailsDependencies,
    FakeStorageReaderThatFails,
)


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
    deps = FakeTripDetailsDependencies(
        pipeline_config=make_pipeline_config(),
        creation_result={
            "table_name": "trip_details",
            "staging_object_name": "gtfs/staging/trip_details.parquet",
            "row_count": 1,
            "staged_written": True,
        },
        storage_buffer=build_trip_details_parquet_buffer(),
        expectations_result={
            "valid_df": pd.DataFrame(),
            "invalid_df": None,
            "expectations_summary": {
                "rows_failed": 0,
                "expectations_with_violations": 0,
                "expectations_failed_due_to_exceptions": 0,
            },
        },
        relocation_result={"status": "SUCCESS", "moved": [], "errors": []},
    )

    orig_load_config = gtfs.gtfs.load_pipeline_config
    orig_create_trip = gtfs.gtfs.create_trip_details_table_and_fill_missing_data
    orig_read_storage = gtfs.gtfs.read_file_from_object_storage_to_bytesio
    orig_validate = gtfs.gtfs.validate_expectations
    orig_relocate = gtfs.gtfs.relocate_staged_trusted_files

    gtfs.gtfs.load_pipeline_config = deps.load_pipeline_config
    gtfs.gtfs.create_trip_details_table_and_fill_missing_data = (
        deps.create_trip_details_table_and_fill_missing_data
    )
    gtfs.gtfs.read_file_from_object_storage_to_bytesio = (
        deps.read_file_from_object_storage_to_bytesio
    )
    gtfs.gtfs.validate_expectations = deps.validate_expectations
    gtfs.gtfs.relocate_staged_trusted_files = deps.relocate_staged_trusted_files

    try:
        result = create_trip_details(make_run_context(), {})
    finally:
        gtfs.gtfs.load_pipeline_config = orig_load_config
        gtfs.gtfs.create_trip_details_table_and_fill_missing_data = orig_create_trip
        gtfs.gtfs.read_file_from_object_storage_to_bytesio = orig_read_storage
        gtfs.gtfs.validate_expectations = orig_validate
        gtfs.gtfs.relocate_staged_trusted_files = orig_relocate

    stage = result["enrichment"]
    assert stage["status"] == "PASS"
    assert stage["relocation_status"] == "SUCCESS"
    assert stage["artifacts"]["column_lineage"]["warning"] is None
    assert "relocation_details" in stage
    assert stage["relocation_details"]["errors"] == []
    assert deps.relocation_calls == [("final", [{"table_name": "trip_details", "staging_object_name": "gtfs/staging/trip_details.parquet", "staged_written": True}])]


def test_create_trip_details_sets_lineage_warning_on_drift():
    deps = FakeTripDetailsDependencies(
        pipeline_config=make_pipeline_config(),
        creation_result={
            "table_name": "trip_details",
            "staging_object_name": "gtfs/staging/trip_details.parquet",
            "row_count": 1,
            "staged_written": True,
        },
        storage_buffer=build_trip_details_parquet_buffer_with_drift(),
        expectations_result={
            "valid_df": pd.DataFrame(),
            "invalid_df": None,
            "expectations_summary": {
                "rows_failed": 0,
                "expectations_with_violations": 0,
                "expectations_failed_due_to_exceptions": 0,
            },
        },
        relocation_result={"status": "SUCCESS", "moved": [], "errors": []},
    )

    orig_load_config = gtfs.gtfs.load_pipeline_config
    orig_create_trip = gtfs.gtfs.create_trip_details_table_and_fill_missing_data
    orig_read_storage = gtfs.gtfs.read_file_from_object_storage_to_bytesio
    orig_validate = gtfs.gtfs.validate_expectations
    orig_relocate = gtfs.gtfs.relocate_staged_trusted_files

    gtfs.gtfs.load_pipeline_config = deps.load_pipeline_config
    gtfs.gtfs.create_trip_details_table_and_fill_missing_data = (
        deps.create_trip_details_table_and_fill_missing_data
    )
    gtfs.gtfs.read_file_from_object_storage_to_bytesio = (
        deps.read_file_from_object_storage_to_bytesio
    )
    gtfs.gtfs.validate_expectations = deps.validate_expectations
    gtfs.gtfs.relocate_staged_trusted_files = deps.relocate_staged_trusted_files

    try:
        result = create_trip_details(make_run_context(), {})
    finally:
        gtfs.gtfs.load_pipeline_config = orig_load_config
        gtfs.gtfs.create_trip_details_table_and_fill_missing_data = orig_create_trip
        gtfs.gtfs.read_file_from_object_storage_to_bytesio = orig_read_storage
        gtfs.gtfs.validate_expectations = orig_validate
        gtfs.gtfs.relocate_staged_trusted_files = orig_relocate

    stage = result["enrichment"]
    assert stage["status"] == "PASS"
    assert stage["artifacts"]["column_lineage"]["warning"] == "lineage drift detected"
    assert "relocation_details" in stage
    assert stage["relocation_details"]["errors"] == []
    assert deps.relocation_calls == [("final", [{"table_name": "trip_details", "staging_object_name": "gtfs/staging/trip_details.parquet", "staged_written": True}])]


def test_create_trip_details_quarantines_on_gx_failure():
    deps = FakeTripDetailsDependencies(
        pipeline_config=make_pipeline_config(),
        creation_result={
            "table_name": "trip_details",
            "staging_object_name": "gtfs/staging/trip_details.parquet",
            "row_count": 1,
            "staged_written": True,
        },
        storage_buffer=build_trip_details_parquet_buffer(),
        expectations_result={
            "valid_df": pd.DataFrame(),
            "invalid_df": pd.DataFrame({"col": [1]}),
            "expectations_summary": {
                "rows_failed": 1,
                "expectations_with_violations": 1,
                "expectations_failed_due_to_exceptions": 0,
            },
        },
        relocation_result={"status": "SUCCESS", "moved": [], "errors": []},
    )

    orig_load_config = gtfs.gtfs.load_pipeline_config
    orig_create_trip = gtfs.gtfs.create_trip_details_table_and_fill_missing_data
    orig_read_storage = gtfs.gtfs.read_file_from_object_storage_to_bytesio
    orig_validate = gtfs.gtfs.validate_expectations
    orig_relocate = gtfs.gtfs.relocate_staged_trusted_files
    orig_handle_error = gtfs.gtfs.handle_unexpected_error

    gtfs.gtfs.load_pipeline_config = deps.load_pipeline_config
    gtfs.gtfs.create_trip_details_table_and_fill_missing_data = (
        deps.create_trip_details_table_and_fill_missing_data
    )
    gtfs.gtfs.read_file_from_object_storage_to_bytesio = (
        deps.read_file_from_object_storage_to_bytesio
    )
    gtfs.gtfs.validate_expectations = deps.validate_expectations
    gtfs.gtfs.relocate_staged_trusted_files = deps.relocate_staged_trusted_files
    gtfs.gtfs.handle_unexpected_error = lambda e, run_context, stage_results, write_fn=None: None

    try:
        with pytest.raises(StageExecutionError) as excinfo:
            create_trip_details(make_run_context(), {})
    finally:
        gtfs.gtfs.load_pipeline_config = orig_load_config
        gtfs.gtfs.create_trip_details_table_and_fill_missing_data = orig_create_trip
        gtfs.gtfs.read_file_from_object_storage_to_bytesio = orig_read_storage
        gtfs.gtfs.validate_expectations = orig_validate
        gtfs.gtfs.relocate_staged_trusted_files = orig_relocate
        gtfs.gtfs.handle_unexpected_error = orig_handle_error

    stage_result = excinfo.value.stage_result
    assert "relocation_details" in stage_result
    assert stage_result["relocation_details"]["errors"] == []
    assert deps.relocation_calls == [("quarantine", [{"table_name": "trip_details", "staging_object_name": "gtfs/staging/trip_details.parquet", "staged_written": True}])]


def test_create_trip_details_quarantines_when_validation_raises_after_staging():
    deps = FakeTripDetailsDependencies(
        pipeline_config=make_pipeline_config(),
        creation_result={
            "table_name": "trip_details",
            "staging_object_name": "gtfs/staging/trip_details.parquet",
            "row_count": 1,
            "staged_written": True,
        },
        storage_buffer=None,
        expectations_result=None,
        relocation_result={"status": "SUCCESS", "moved": [], "errors": []},
    )
    deps.storage_reader = FakeStorageReaderThatFails()

    orig_load_config = gtfs.gtfs.load_pipeline_config
    orig_create_trip = gtfs.gtfs.create_trip_details_table_and_fill_missing_data
    orig_read_storage = gtfs.gtfs.read_file_from_object_storage_to_bytesio
    orig_relocate = gtfs.gtfs.relocate_staged_trusted_files
    orig_handle_error = gtfs.gtfs.handle_unexpected_error

    gtfs.gtfs.load_pipeline_config = deps.load_pipeline_config
    gtfs.gtfs.create_trip_details_table_and_fill_missing_data = (
        deps.create_trip_details_table_and_fill_missing_data
    )
    gtfs.gtfs.read_file_from_object_storage_to_bytesio = (
        deps.read_file_from_object_storage_to_bytesio
    )
    gtfs.gtfs.relocate_staged_trusted_files = deps.relocate_staged_trusted_files
    gtfs.gtfs.handle_unexpected_error = lambda e, run_context, stage_results, write_fn=None: None

    try:
        with pytest.raises(StageExecutionError) as excinfo:
            create_trip_details(make_run_context(), {})
    finally:
        gtfs.gtfs.load_pipeline_config = orig_load_config
        gtfs.gtfs.create_trip_details_table_and_fill_missing_data = orig_create_trip
        gtfs.gtfs.read_file_from_object_storage_to_bytesio = orig_read_storage
        gtfs.gtfs.relocate_staged_trusted_files = orig_relocate
        gtfs.gtfs.handle_unexpected_error = orig_handle_error

    stage_result = excinfo.value.stage_result
    assert "relocation_details" in stage_result
    assert stage_result["relocation_details"]["errors"] == []
    assert deps.relocation_calls == [("quarantine", [{"table_name": "trip_details", "staging_object_name": "gtfs/staging/trip_details.parquet", "staged_written": True}])]
