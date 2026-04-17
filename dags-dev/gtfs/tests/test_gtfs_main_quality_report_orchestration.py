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


def test_main_persists_single_consolidated_success_report():
    module = load_gtfs_v3_module()
    calls = {"success": 0, "failure": 0}

    module._load_pipeline_config = lambda: make_pipeline_config()
    module.extract_load_files = lambda cfg: {"status": "PASS", "validated_items_count": 1, "error_details": {}}
    module.transform = lambda cfg: {"status": "PASS", "validated_items_count": 6, "error_details": {}}
    module.create_trip_details = lambda cfg: {"status": "PASS", "validated_items_count": 0, "error_details": {}}
    module.create_data_quality_report = lambda **kwargs: calls.__setitem__("success", calls["success"] + 1) or {"summary": {}}
    module.create_failure_quality_report = lambda **kwargs: calls.__setitem__("failure", calls["failure"] + 1) or {"summary": {}}
    module._send_webhook_from_report = lambda report, config, path: None

    module.main()

    assert calls["success"] == 1
    assert calls["failure"] == 0


def test_main_persists_single_consolidated_failure_report():
    module = load_gtfs_v3_module()
    calls = {"success": 0, "failure": 0}

    module._load_pipeline_config = lambda: make_pipeline_config()
    module.extract_load_files = lambda cfg: {"status": "PASS", "validated_items_count": 1, "error_details": {}}
    module.transform = lambda cfg: (_ for _ in ()).throw(
        module.StageExecutionError(
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
    )
    module.create_data_quality_report = lambda **kwargs: calls.__setitem__("success", calls["success"] + 1) or {"summary": {}}
    module.create_failure_quality_report = lambda **kwargs: calls.__setitem__("failure", calls["failure"] + 1) or {"summary": {}}
    module._send_webhook_from_report = lambda report, config, path: None

    with pytest.raises(module.StageExecutionError):
        module.main()

    assert calls["success"] == 0
    assert calls["failure"] == 1
