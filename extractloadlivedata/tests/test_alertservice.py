import json

from src.infra.alertservice_client import build_alert_payload, send_alert


def test_build_alert_payload_wraps_summary():
    summary = {
        "pipeline": "extractloadlivedata",
        "execution_id": "exec-1",
        "status": "PASS",
    }
    payload = build_alert_payload(summary)
    assert json.loads(payload.decode("utf-8")) == {"summary": summary}


def test_send_alert_skips_when_webhook_disabled():
    summary = {
        "pipeline": "extractloadlivedata",
        "execution_id": "exec-2",
        "status": "WARN",
    }
    send_alert("disabled", summary)
    send_alert("none", summary)
    send_alert("null", summary)
    send_alert("", summary)


def test_send_alert_is_non_blocking_on_connection_error():
    summary = {
        "pipeline": "extractloadlivedata",
        "execution_id": "exec-3",
        "status": "FAIL",
    }
    # Invalid URL format fails deterministically without requiring network.
    send_alert("not-a-valid-url", summary)
