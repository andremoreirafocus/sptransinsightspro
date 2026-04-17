import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[2] / "gtfs-v3.py"


def load_gtfs_v3_module():
    spec = importlib.util.spec_from_file_location("gtfs_v3_module", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_report():
    return {
        "summary": {
            "execution_id": "exec-1",
            "status": "FAIL",
            "failure_phase": "transformation",
        }
    }


def make_config(webhook_url):
    return {
        "general": {
            "notifications": {"webhook_url": webhook_url},
        }
    }


def test_webhook_disabled_for_empty_none_and_disabled_values():
    module = load_gtfs_v3_module()
    calls = []
    module.send_webhook = lambda summary, webhook_url: calls.append((summary, webhook_url))

    for value in [None, "", "disabled", "none", "null", " DISABLED "]:
        module._send_webhook_from_report(make_report(), make_config(value), "failure path")

    assert calls == []


def test_webhook_sends_when_url_is_present_and_trimmed():
    module = load_gtfs_v3_module()
    calls = []
    module.send_webhook = lambda summary, webhook_url: calls.append((summary, webhook_url))

    module._send_webhook_from_report(
        make_report(),
        make_config(" http://localhost:8000/notify "),
        "success path",
    )

    assert len(calls) == 1
    assert calls[0][1] == "http://localhost:8000/notify"
