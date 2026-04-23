import pytest
import gtfs.gtfs
from gtfs.gtfs import transform, StageExecutionError
from gtfs.tests.fakes.fake_orchestration_dependencies import (
    FakeTransformationDependencies,
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


def test_transformation_stage_quarantines_staged_files_on_validation_failure():
    results_by_table = {
        "stops": {
            "table_name": "stops",
            "staging_object_name": "gtfs/staging/stops.parquet",
            "is_valid": False,
            "errors": ["gx_validation_failed"],
            "expectations_summary": {"rows_failed": 1},
            "staged_written": True,
        },
        "stop_times": {
            "table_name": "stop_times",
            "staging_object_name": "gtfs/staging/stop_times.parquet",
            "is_valid": True,
            "errors": [],
            "expectations_summary": {"rows_failed": 0},
            "staged_written": True,
        },
        "routes": {
            "table_name": "routes",
            "staging_object_name": "gtfs/staging/routes.parquet",
            "is_valid": False,
            "errors": ["load_failed:boom"],
            "expectations_summary": None,
            "staged_written": False,
        },
        "trips": {
            "table_name": "trips",
            "staging_object_name": "gtfs/staging/trips.parquet",
            "is_valid": True,
            "errors": [],
            "expectations_summary": None,
            "staged_written": True,
        },
        "frequencies": {
            "table_name": "frequencies",
            "staging_object_name": "gtfs/staging/frequencies.parquet",
            "is_valid": True,
            "errors": [],
            "expectations_summary": None,
            "staged_written": True,
        },
        "calendar": {
            "table_name": "calendar",
            "staging_object_name": "gtfs/staging/calendar.parquet",
            "is_valid": True,
            "errors": [],
            "expectations_summary": None,
            "staged_written": True,
        },
    }

    deps = FakeTransformationDependencies(
        pipeline_config=make_pipeline_config(),
        results_by_table=results_by_table,
        relocation_result={"status": "SUCCESS", "moved": [], "errors": []},
    )

    orig_load_config = gtfs.gtfs.load_pipeline_config
    orig_transform_table = gtfs.gtfs.transform_and_validate_table
    orig_relocate = gtfs.gtfs.relocate_staged_trusted_files

    gtfs.gtfs.load_pipeline_config = deps.load_pipeline_config
    gtfs.gtfs.transform_and_validate_table = deps.transform_and_validate_table
    gtfs.gtfs.relocate_staged_trusted_files = deps.relocate_staged_trusted_files

    try:
        with pytest.raises(StageExecutionError, match="Validation failures detected") as excinfo:
            transform(make_run_context(), {}, write_fn=fake_write_fn)
    finally:
        gtfs.gtfs.load_pipeline_config = orig_load_config
        gtfs.gtfs.transform_and_validate_table = orig_transform_table
        gtfs.gtfs.relocate_staged_trusted_files = orig_relocate

    assert len(deps.relocation_calls) == 1
    assert deps.relocation_calls[0][0] == "quarantine"
    staged_tables = {row["table_name"] for row in deps.relocation_calls[0][1]}
    assert "routes" not in staged_tables
    assert "stops" in staged_tables
    stage_result = excinfo.value.stage_result
    assert "relocation_details" in stage_result
    assert "moved" in stage_result["relocation_details"]
    assert "errors" in stage_result["relocation_details"]


def test_transformation_stage_moves_staged_files_to_final_on_success():
    results_by_table = {
        table_name: {
            "table_name": table_name,
            "staging_object_name": f"gtfs/staging/{table_name}.parquet",
            "is_valid": True,
            "errors": [],
            "expectations_summary": None,
            "staged_written": True,
        }
        for table_name in ["stops", "stop_times", "routes", "trips", "frequencies", "calendar"]
    }
    deps = FakeTransformationDependencies(
        pipeline_config=make_pipeline_config(),
        results_by_table=results_by_table,
        relocation_result={"status": "SUCCESS", "moved": [], "errors": []},
    )

    orig_load_config = gtfs.gtfs.load_pipeline_config
    orig_transform_table = gtfs.gtfs.transform_and_validate_table
    orig_relocate = gtfs.gtfs.relocate_staged_trusted_files

    gtfs.gtfs.load_pipeline_config = deps.load_pipeline_config
    gtfs.gtfs.transform_and_validate_table = deps.transform_and_validate_table
    gtfs.gtfs.relocate_staged_trusted_files = deps.relocate_staged_trusted_files

    try:
        result = transform(make_run_context(), {}, write_fn=fake_write_fn)
    finally:
        gtfs.gtfs.load_pipeline_config = orig_load_config
        gtfs.gtfs.transform_and_validate_table = orig_transform_table
        gtfs.gtfs.relocate_staged_trusted_files = orig_relocate

    assert len(deps.relocation_calls) == 1
    assert deps.relocation_calls[0][0] == "final"
    assert len(deps.relocation_calls[0][1]) == 6
    assert result["transformation"]["status"] == "PASS"
    assert result["transformation"]["validated_items_count"] == 6
    assert result["transformation"]["relocation_status"] == "SUCCESS"
    assert "relocation_details" in result["transformation"]
    assert result["transformation"]["relocation_details"]["errors"] == []
