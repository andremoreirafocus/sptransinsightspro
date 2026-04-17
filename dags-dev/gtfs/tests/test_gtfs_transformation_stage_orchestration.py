import importlib.util
from pathlib import Path

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
            "notifications": {"webhook_url": "disabled"},
        }
    }


def test_transformation_stage_quarantines_staged_files_on_validation_failure(monkeypatch):
    module = load_gtfs_v3_module()
    relocation_calls = []

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

    monkeypatch.setattr(module, "_load_pipeline_config", lambda: make_pipeline_config())
    monkeypatch.setattr(
        module,
        "transform_and_validate_table",
        lambda cfg, table_name: results_by_table[table_name],
    )

    def fake_relocate(cfg, staged_results, target):
        relocation_calls.append((target, staged_results))
        return {"status": "SUCCESS", "moved": [], "errors": []}

    monkeypatch.setattr(module, "relocate_staged_trusted_files", fake_relocate)

    with pytest.raises(ValueError, match="Validation failures detected"):
        module.transform()

    assert len(relocation_calls) == 1
    assert relocation_calls[0][0] == "quarantine"
    staged_tables = {row["table_name"] for row in relocation_calls[0][1]}
    assert "routes" not in staged_tables
    assert "stops" in staged_tables


def test_transformation_stage_moves_staged_files_to_final_on_success(monkeypatch):
    module = load_gtfs_v3_module()
    relocation_calls = []
    monkeypatch.setattr(module, "_load_pipeline_config", lambda: make_pipeline_config())
    monkeypatch.setattr(
        module,
        "transform_and_validate_table",
        lambda cfg, table_name: {
            "table_name": table_name,
            "staging_object_name": f"gtfs/staging/{table_name}.parquet",
            "is_valid": True,
            "errors": [],
            "expectations_summary": None,
            "staged_written": True,
        },
    )

    def fake_relocate(cfg, staged_results, target):
        relocation_calls.append((target, staged_results))
        return {"status": "SUCCESS", "moved": [], "errors": []}

    monkeypatch.setattr(module, "relocate_staged_trusted_files", fake_relocate)

    module.transform()

    assert len(relocation_calls) == 1
    assert relocation_calls[0][0] == "final"
    assert len(relocation_calls[0][1]) == 6
