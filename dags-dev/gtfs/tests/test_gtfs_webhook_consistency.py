import gtfs.gtfs
from gtfs.gtfs import send_webhook_from_report


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
    calls = []
    orig_send_webhook = gtfs.gtfs.send_webhook
    gtfs.gtfs.send_webhook = lambda summary, webhook_url: calls.append((summary, webhook_url))

    try:
        for value in [None, "", "disabled", "none", "null", " DISABLED "]:
            send_webhook_from_report(make_report(), make_config(value), "failure path")
    finally:
        gtfs.gtfs.send_webhook = orig_send_webhook

    assert calls == []


def test_webhook_sends_when_url_is_present_and_trimmed():
    calls = []
    orig_send_webhook = gtfs.gtfs.send_webhook
    gtfs.gtfs.send_webhook = lambda summary, webhook_url: calls.append((summary, webhook_url))

    try:
        send_webhook_from_report(
            make_report(),
            make_config(" http://localhost:8000/notify "),
            "success path",
        )
    finally:
        gtfs.gtfs.send_webhook = orig_send_webhook

    assert len(calls) == 1
    assert calls[0][1] == "http://localhost:8000/notify"
