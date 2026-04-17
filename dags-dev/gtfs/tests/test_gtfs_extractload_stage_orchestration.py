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


def test_extract_load_files_quarantines_and_raises_on_validation_failure(monkeypatch):
    module = load_gtfs_v3_module()
    calls = {"failed": 0, "raw": 0}

    monkeypatch.setattr(module, "_load_pipeline_config", lambda: make_pipeline_config())
    monkeypatch.setattr(module, "extract_gtfs_files", lambda cfg: ["stops.txt", "routes.txt"])
    monkeypatch.setattr(
        module,
        "validate_raw_gtfs_files",
        lambda cfg, files: {
            "is_valid": False,
            "errors_by_file": {"stops.txt": ["insufficient_lines"]},
            "validated_files_count": 2,
        },
    )

    def fake_raw_save(cfg, files, failed=False):
        if failed:
            calls["failed"] += 1
        else:
            calls["raw"] += 1

    monkeypatch.setattr(module, "save_files_to_raw_storage", fake_raw_save)

    with pytest.raises(ValueError, match="Raw GTFS validation failed"):
        module.extract_load_files()

    assert calls["failed"] == 1
    assert calls["raw"] == 0


def test_extract_load_files_saves_raw_on_success(monkeypatch):
    module = load_gtfs_v3_module()
    calls = {"failed": 0, "raw": 0}

    monkeypatch.setattr(module, "_load_pipeline_config", lambda: make_pipeline_config())
    monkeypatch.setattr(module, "extract_gtfs_files", lambda cfg: ["stops.txt"])
    monkeypatch.setattr(
        module,
        "validate_raw_gtfs_files",
        lambda cfg, files: {
            "is_valid": True,
            "errors_by_file": {},
            "validated_files_count": 1,
        },
    )

    def fake_raw_save(cfg, files, failed=False):
        if failed:
            calls["failed"] += 1
        else:
            calls["raw"] += 1

    monkeypatch.setattr(module, "save_files_to_raw_storage", fake_raw_save)

    module.extract_load_files()

    assert calls["failed"] == 0
    assert calls["raw"] == 1
