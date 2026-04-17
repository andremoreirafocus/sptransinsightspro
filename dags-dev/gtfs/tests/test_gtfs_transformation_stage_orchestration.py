import importlib.util
from pathlib import Path

import pytest
from gtfs.tests.fakes.fake_orchestration_dependencies import (
    FakeTransformationDependencies,
)


MODULE_PATH = Path(__file__).resolve().parents[2] / "gtfs-v3.py"


def load_gtfs_v3_module():
    spec = importlib.util.spec_from_file_location("gtfs_v3_module", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_pipeline_config():
    return {
        "general": {
            "notifications": {"webhook_url": "disabled"},
        }
    }


def test_transformation_stage_quarantines_staged_files_on_validation_failure():
    module = load_gtfs_v3_module()

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
    module._load_pipeline_config = deps.load_pipeline_config
    module.transform_and_validate_table = deps.transform_and_validate_table
    module.relocate_staged_trusted_files = deps.relocate_staged_trusted_files

    with pytest.raises(Exception, match="Validation failures detected") as excinfo:
        module.transform()

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
    module = load_gtfs_v3_module()
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
    module._load_pipeline_config = deps.load_pipeline_config
    module.transform_and_validate_table = deps.transform_and_validate_table
    module.relocate_staged_trusted_files = deps.relocate_staged_trusted_files
    result = module.transform()

    assert len(deps.relocation_calls) == 1
    assert deps.relocation_calls[0][0] == "final"
    assert len(deps.relocation_calls[0][1]) == 6
    assert result["status"] == "PASS"
    assert result["validated_items_count"] == 6
    assert result["relocation_status"] == "SUCCESS"
    assert "relocation_details" in result
    assert result["relocation_details"]["errors"] == []
