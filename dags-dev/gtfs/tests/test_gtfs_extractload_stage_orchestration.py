import importlib.util
from pathlib import Path

import pytest
from gtfs.tests.fakes.fake_orchestration_dependencies import (
    FakeExtractLoadDependencies,
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


def test_extract_load_files_quarantines_and_raises_on_validation_failure():
    module = load_gtfs_v3_module()
    deps = FakeExtractLoadDependencies(
        pipeline_config=make_pipeline_config(),
        extracted_files=["stops.txt", "routes.txt"],
        validation_result={
            "is_valid": False,
            "errors_by_file": {"stops.txt": ["insufficient_lines"]},
            "validated_files_count": 2,
        },
    )
    module._load_pipeline_config = deps.load_pipeline_config
    module.extract_gtfs_files = deps.extract_gtfs_files
    module.validate_raw_gtfs_files = deps.validate_raw_gtfs_files
    module.save_files_to_raw_storage = deps.save_files_to_raw_storage

    with pytest.raises(ValueError, match="Raw GTFS validation failed"):
        module.extract_load_files()

    assert len(deps.raw_save_calls) == 1
    assert deps.raw_save_calls[0]["failed"] is True


def test_extract_load_files_saves_raw_on_success():
    module = load_gtfs_v3_module()
    deps = FakeExtractLoadDependencies(
        pipeline_config=make_pipeline_config(),
        extracted_files=["stops.txt"],
        validation_result={
            "is_valid": True,
            "errors_by_file": {},
            "validated_files_count": 1,
        },
    )
    module._load_pipeline_config = deps.load_pipeline_config
    module.extract_gtfs_files = deps.extract_gtfs_files
    module.validate_raw_gtfs_files = deps.validate_raw_gtfs_files
    module.save_files_to_raw_storage = deps.save_files_to_raw_storage
    result = module.extract_load_files()

    assert len(deps.raw_save_calls) == 1
    assert deps.raw_save_calls[0]["failed"] is False
    assert result["status"] == "PASS"
    assert result["validated_items_count"] == 1
