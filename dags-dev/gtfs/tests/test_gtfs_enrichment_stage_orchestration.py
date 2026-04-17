import importlib.util
from io import BytesIO
from pathlib import Path

import pandas as pd
import pytest


MODULE_PATH = Path(__file__).resolve().parents[2] / "gtfs-v3.py"


def load_gtfs_v3_module():
    spec = importlib.util.spec_from_file_location("gtfs_v3_module", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
    module = load_gtfs_v3_module()
    relocate_calls = []
    config = make_pipeline_config()

    module._load_pipeline_config = lambda: config
    module.create_trip_details_table_and_fill_missing_data = lambda cfg: {
        "table_name": "trip_details",
        "staging_object_name": "gtfs/staging/trip_details.parquet",
        "row_count": 1,
        "staged_written": True,
    }
    module.read_file_from_object_storage_to_bytesio = (
        lambda connection, bucket_name, object_name: build_trip_details_parquet_buffer()
    )
    module.validate_expectations = lambda df, suite: {
        "valid_df": df,
        "invalid_df": None,
        "expectations_summary": {
            "rows_failed": 0,
            "expectations_with_violations": 0,
            "expectations_failed_due_to_exceptions": 0,
        },
    }
    module.relocate_staged_trusted_files = (
        lambda cfg, staged_results, target: relocate_calls.append(target)
        or {"status": "SUCCESS", "moved": [], "errors": []}
    )

    result = module.create_trip_details(config)

    assert result["status"] == "PASS"
    assert result["relocation_status"] == "SUCCESS"
    assert result["artifacts"]["column_lineage"]["warning"] is None
    assert "relocation_details" in result
    assert result["relocation_details"]["errors"] == []
    assert relocate_calls == ["final"]


def test_create_trip_details_sets_lineage_warning_on_drift():
    module = load_gtfs_v3_module()
    relocate_calls = []
    config = make_pipeline_config()

    module._load_pipeline_config = lambda: config
    module.create_trip_details_table_and_fill_missing_data = lambda cfg: {
        "table_name": "trip_details",
        "staging_object_name": "gtfs/staging/trip_details.parquet",
        "row_count": 1,
        "staged_written": True,
    }
    module.read_file_from_object_storage_to_bytesio = (
        lambda connection, bucket_name, object_name: build_trip_details_parquet_buffer_with_drift()
    )
    module.validate_expectations = lambda df, suite: {
        "valid_df": df,
        "invalid_df": None,
        "expectations_summary": {
            "rows_failed": 0,
            "expectations_with_violations": 0,
            "expectations_failed_due_to_exceptions": 0,
        },
    }
    module.relocate_staged_trusted_files = (
        lambda cfg, staged_results, target: relocate_calls.append(target)
        or {"status": "SUCCESS", "moved": [], "errors": []}
    )

    result = module.create_trip_details(config)

    assert result["status"] == "PASS"
    assert result["artifacts"]["column_lineage"]["warning"] == "lineage drift detected"
    assert "relocation_details" in result
    assert result["relocation_details"]["errors"] == []
    assert relocate_calls == ["final"]


def test_create_trip_details_quarantines_on_gx_failure():
    module = load_gtfs_v3_module()
    relocate_calls = []
    config = make_pipeline_config()

    module._load_pipeline_config = lambda: config
    module.create_trip_details_table_and_fill_missing_data = lambda cfg: {
        "table_name": "trip_details",
        "staging_object_name": "gtfs/staging/trip_details.parquet",
        "row_count": 1,
        "staged_written": True,
    }
    module.read_file_from_object_storage_to_bytesio = (
        lambda connection, bucket_name, object_name: build_trip_details_parquet_buffer()
    )
    module.validate_expectations = lambda df, suite: {
        "valid_df": df.iloc[0:0],
        "invalid_df": df,
        "expectations_summary": {
            "rows_failed": 1,
            "expectations_with_violations": 1,
            "expectations_failed_due_to_exceptions": 0,
        },
    }
    module.relocate_staged_trusted_files = (
        lambda cfg, staged_results, target: relocate_calls.append(target)
        or {"status": "SUCCESS", "moved": [], "errors": []}
    )

    with pytest.raises(module.StageExecutionError) as excinfo:
        module.create_trip_details(config)

    stage_result = excinfo.value.stage_result
    assert "relocation_details" in stage_result
    assert stage_result["relocation_details"]["errors"] == []
    assert relocate_calls == ["quarantine"]


def test_create_trip_details_quarantines_when_validation_raises_after_staging():
    module = load_gtfs_v3_module()
    relocate_calls = []
    config = make_pipeline_config()

    module._load_pipeline_config = lambda: config
    module.create_trip_details_table_and_fill_missing_data = lambda cfg: {
        "table_name": "trip_details",
        "staging_object_name": "gtfs/staging/trip_details.parquet",
        "row_count": 1,
        "staged_written": True,
    }
    module.read_file_from_object_storage_to_bytesio = (
        lambda connection, bucket_name, object_name: (_ for _ in ()).throw(
            RuntimeError("read failed")
        )
    )
    module.relocate_staged_trusted_files = (
        lambda cfg, staged_results, target: relocate_calls.append(target)
        or {"status": "SUCCESS", "moved": [], "errors": []}
    )

    with pytest.raises(module.StageExecutionError) as excinfo:
        module.create_trip_details(config)

    stage_result = excinfo.value.stage_result
    assert "relocation_details" in stage_result
    assert stage_result["relocation_details"]["errors"] == []
    assert relocate_calls == ["quarantine"]
