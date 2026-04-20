import pytest

from src.infra.notifier import format_summary
from src.process_notification import process_notification
from tests.fakes.fake_email_sender import FakeEmailSender
from tests.fakes.fake_query_window import FakeQueryWindow
from tests.fakes.fake_summary_store import FakeSummaryStore


def run(summary, pipeline_config, store=None, query=None, sender=None):
    return process_notification(
        summary=summary,
        pipeline_config=pipeline_config,
        subject_prefix="[DQ] status update for pipeline",
        store_summary=store or FakeSummaryStore(),
        query_window=query or FakeQueryWindow(),
        send_email=sender or FakeEmailSender(),
        format_summary=format_summary,
    )


def test_missing_summary_raises(pipeline_config):
    with pytest.raises(ValueError, match="summary is required"):
        run({}, pipeline_config)


def test_missing_pipeline_field_raises(pipeline_config):
    with pytest.raises(ValueError, match="pipeline"):
        run({"status": "FAIL"}, pipeline_config)


def test_missing_status_field_raises(pipeline_config):
    with pytest.raises(ValueError, match="status"):
        run({"pipeline": "transformlivedata"}, pipeline_config)


def test_unknown_status_ignored(pipeline_config, valid_summary_fail):
    summary = {**valid_summary_fail, "status": "UNKNOWN"}
    result = run(summary, pipeline_config)
    assert result == {"status": "ignored", "reason": "unknown_status"}


def test_pass_stored_no_email(pipeline_config, valid_summary_pass):
    store = FakeSummaryStore()
    sender = FakeEmailSender()
    result = run(valid_summary_pass, pipeline_config, store=store, sender=sender)
    assert result == {"status": "stored", "reason": "pass_status"}
    assert len(store.stored) == 1
    assert len(sender.sent) == 0


def test_unknown_pipeline_ignored(pipeline_config, valid_summary_fail):
    summary = {**valid_summary_fail, "pipeline": "unknown_pipeline"}
    result = run(summary, pipeline_config)
    assert result == {"status": "ignored", "reason": "unknown_pipeline"}


def test_fail_sends_email(pipeline_config, valid_summary_fail):
    store = FakeSummaryStore()
    sender = FakeEmailSender()
    result = run(valid_summary_fail, pipeline_config, store=store, sender=sender)
    assert result == {"status": "sent", "reason": "fail"}
    assert len(store.stored) == 1
    assert len(sender.sent) == 1


def test_fail_notify_disabled(pipeline_config, valid_summary_fail):
    config = {**pipeline_config}
    config["transformlivedata"] = {
        **config["transformlivedata"],
        "notify_on_fail": False,
    }
    sender = FakeEmailSender()
    result = run(valid_summary_fail, config, sender=sender)
    assert result == {"status": "stored"}
    assert len(sender.sent) == 0


def test_warn_below_threshold_stored(pipeline_config, valid_summary_warn):
    sender = FakeEmailSender()
    query = FakeQueryWindow(rows=[])
    result = run(valid_summary_warn, pipeline_config, query=query, sender=sender)
    assert result == {"status": "stored"}
    assert len(sender.sent) == 0


def test_warn_above_threshold_sends_email(pipeline_config, valid_summary_warn):
    sender = FakeEmailSender()
    rows = [
        {"status": "WARN", "items_failed": 200, "acceptance_rate": 0.98},
        {"status": "WARN", "items_failed": 200, "acceptance_rate": 0.98},
        {"status": "WARN", "items_failed": 200, "acceptance_rate": 0.98},
    ]
    query = FakeQueryWindow(rows=rows)
    result = run(valid_summary_warn, pipeline_config, query=query, sender=sender)
    assert result == {"status": "sent", "reason": "cumulative_warn"}
    assert len(sender.sent) == 1


def test_subject_format(pipeline_config, valid_summary_fail):
    sender = FakeEmailSender()
    run(valid_summary_fail, pipeline_config, sender=sender)
    subject, _ = sender.sent[0]
    assert subject == "[DQ] status update for pipeline transformlivedata: FAIL"


def test_warn_notify_disabled_stored(pipeline_config, valid_summary_warn):
    config = {**pipeline_config}
    config["transformlivedata"] = {
        **config["transformlivedata"],
        "notify_on_warn": False,
    }
    rows = [
        {"status": "WARN", "items_failed": 200, "acceptance_rate": 0.98},
        {"status": "WARN", "items_failed": 200, "acceptance_rate": 0.98},
        {"status": "WARN", "items_failed": 200, "acceptance_rate": 0.98},
    ]
    query = FakeQueryWindow(rows=rows)
    sender = FakeEmailSender()
    result = run(valid_summary_warn, config, query=query, sender=sender)
    assert result == {"status": "stored"}
    assert len(sender.sent) == 0
