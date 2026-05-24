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
    send_fn = lambda summary, webhook_url: calls.append((summary, webhook_url))

    for value in [None, "", "disabled", "none", "null", " DISABLED "]:
        send_webhook_from_report(make_report(), make_config(value), "failure path", send_fn=send_fn)

    assert calls == []


def test_webhook_sends_when_url_is_present_and_trimmed():
    calls = []
    send_fn = lambda summary, webhook_url: calls.append((summary, webhook_url))

    send_webhook_from_report(
        make_report(),
        make_config(" http://localhost:8000/notify "),
        "success path",
        send_fn=send_fn,
    )

    assert len(calls) == 1
    assert calls[0][1] == "http://localhost:8000/notify"
