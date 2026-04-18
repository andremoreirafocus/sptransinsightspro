import json
import pytest
from quality.reporting import (
    build_quality_report_path,
    build_quality_summary,
    create_failure_quality_report,
    save_quality_report,
    save_and_notify_quality_report,
)


# --- build_quality_report_path ---


def test_path_starts_with_bucket():
    path = build_quality_report_path(
        metadata_bucket="meta-bucket",
        quality_report_folder="quality-reports",
        pipeline_name="gtfs",
        batch_ts="2026-04-17T14:10:00",
    )
    assert path.startswith("meta-bucket/")


def test_path_contains_pipeline_subdirectory():
    path = build_quality_report_path(
        metadata_bucket="meta",
        quality_report_folder="quality-reports",
        pipeline_name="gtfs",
        batch_ts="2026-04-17T14:10:00",
    )
    assert "quality-reports/gtfs/" in path


def test_path_partition_uses_all_date_components():
    path = build_quality_report_path(
        metadata_bucket="meta",
        quality_report_folder="quality-reports",
        pipeline_name="gtfs",
        batch_ts="2026-04-17T14:10:00",
    )
    assert "year=2026" in path
    assert "month=04" in path
    assert "day=17" in path
    assert "hour=14" in path


def test_path_filename_contains_hhmm():
    path = build_quality_report_path(
        metadata_bucket="meta",
        quality_report_folder="quality-reports",
        pipeline_name="gtfs",
        batch_ts="2026-04-17T14:10:00",
    )
    assert "quality-report-gtfs_1410.json" in path


def test_path_filename_suffix_is_appended():
    path = build_quality_report_path(
        metadata_bucket="meta",
        quality_report_folder="quality-reports",
        pipeline_name="gtfs",
        batch_ts="2026-04-17T14:10:00",
        filename_suffix="_exec1234",
    )
    assert path.endswith("quality-report-gtfs_1410_exec1234.json")


def test_path_without_suffix_ends_with_json():
    path = build_quality_report_path(
        metadata_bucket="meta",
        quality_report_folder="quality-reports",
        pipeline_name="mypipeline",
        batch_ts="2026-01-01T00:00:00",
    )
    assert path.endswith(".json")
    assert "_" not in path.split("/")[-1].replace("quality-report-mypipeline_0000.json", "")


# --- build_quality_summary ---


def test_summary_contains_all_required_fields():
    summary = build_quality_summary(
        pipeline="gtfs",
        execution_id="exec-1",
        status="PASS",
        acceptance_rate=1.0,
        rows_failed=0,
        quality_report_path="meta/report.json",
    )
    required = {
        "contract_version",
        "pipeline",
        "execution_id",
        "status",
        "acceptance_rate",
        "rows_failed",
        "quality_report_path",
        "failure_phase",
        "failure_message",
        "generated_at_utc",
    }
    assert required.issubset(summary.keys())


def test_summary_contract_version_is_v1():
    summary = build_quality_summary(
        pipeline="gtfs",
        execution_id="exec-1",
        status="PASS",
        acceptance_rate=1.0,
        rows_failed=0,
        quality_report_path="meta/report.json",
    )
    assert summary["contract_version"] == "v1"


def test_summary_extra_fields_are_merged_flat():
    summary = build_quality_summary(
        pipeline="gtfs",
        execution_id="exec-1",
        status="PASS",
        acceptance_rate=1.0,
        rows_failed=0,
        quality_report_path="meta/report.json",
        stage="pipeline",
        validated_items_count=42,
    )
    assert summary["stage"] == "pipeline"
    assert summary["validated_items_count"] == 42


def test_summary_failure_phase_and_message_default_to_none():
    summary = build_quality_summary(
        pipeline="gtfs",
        execution_id="exec-1",
        status="PASS",
        acceptance_rate=1.0,
        rows_failed=0,
        quality_report_path="meta/report.json",
    )
    assert summary["failure_phase"] is None
    assert summary["failure_message"] is None


def test_summary_accepts_all_three_status_values():
    for status in ("PASS", "WARN", "FAIL"):
        summary = build_quality_summary(
            pipeline="p",
            execution_id="e",
            status=status,
            acceptance_rate=0.9,
            rows_failed=1,
            quality_report_path="p",
        )
        assert summary["status"] == status


def test_summary_pipeline_field_matches_input():
    summary = build_quality_summary(
        pipeline="transformlivedata",
        execution_id="e",
        status="PASS",
        acceptance_rate=1.0,
        rows_failed=0,
        quality_report_path="p",
    )
    assert summary["pipeline"] == "transformlivedata"


# --- create_failure_quality_report ---


def test_failure_report_has_summary_and_details_keys():
    report = create_failure_quality_report(
        pipeline="gtfs",
        execution_id="exec-1",
        failure_phase="transformation",
        failure_message="Something broke",
        quality_report_path="meta/report.json",
    )
    assert "summary" in report
    assert "details" in report


def test_failure_report_summary_status_is_fail():
    report = create_failure_quality_report(
        pipeline="gtfs",
        execution_id="exec-1",
        failure_phase="transformation",
        failure_message="Something broke",
        quality_report_path="meta/report.json",
    )
    assert report["summary"]["status"] == "FAIL"


def test_failure_report_summary_carries_failure_phase_and_message():
    report = create_failure_quality_report(
        pipeline="gtfs",
        execution_id="exec-1",
        failure_phase="enrichment",
        failure_message="GX exception",
        quality_report_path="meta/report.json",
    )
    assert report["summary"]["failure_phase"] == "enrichment"
    assert report["summary"]["failure_message"] == "GX exception"


def test_failure_report_acceptance_rate_is_zero():
    report = create_failure_quality_report(
        pipeline="gtfs",
        execution_id="exec-1",
        failure_phase="load",
        failure_message="err",
        quality_report_path="meta/report.json",
    )
    assert report["summary"]["acceptance_rate"] == 0.0


def test_failure_report_details_contains_execution_id_and_status():
    report = create_failure_quality_report(
        pipeline="gtfs",
        execution_id="exec-42",
        failure_phase="load",
        failure_message="err",
        quality_report_path="meta/report.json",
    )
    assert report["details"]["execution_id"] == "exec-42"
    assert report["details"]["status"] == "FAIL"


def test_failure_report_extra_fields_merged_into_summary():
    report = create_failure_quality_report(
        pipeline="gtfs",
        execution_id="exec-1",
        failure_phase="load",
        failure_message="err",
        quality_report_path="meta/report.json",
        stage="pipeline",
        relocation_status="NOT_APPLICABLE",
    )
    assert report["summary"]["stage"] == "pipeline"
    assert report["summary"]["relocation_status"] == "NOT_APPLICABLE"


# --- save_quality_report ---


def test_save_quality_report_calls_write_fn_once():
    calls = []

    def fake_write(connection_data, buffer, bucket_name, object_name):
        calls.append((bucket_name, object_name))

    save_quality_report(
        report=_make_report(),
        path="meta-bucket/quality-reports/gtfs/report.json",
        connection_data=_make_connection(),
        write_fn=fake_write,
    )
    assert len(calls) == 1


def test_save_quality_report_splits_path_correctly():
    calls = []

    def fake_write(connection_data, buffer, bucket_name, object_name):
        calls.append((bucket_name, object_name))

    save_quality_report(
        report=_make_report(),
        path="my-bucket/folder/sub/report.json",
        connection_data=_make_connection(),
        write_fn=fake_write,
    )
    assert calls[0][0] == "my-bucket"
    assert calls[0][1] == "folder/sub/report.json"


def test_save_quality_report_buffer_is_valid_json():
    received = []

    def fake_write(connection_data, buffer, bucket_name, object_name):
        received.append(buffer)

    save_quality_report(
        report=_make_report(),
        path="bucket/report.json",
        connection_data=_make_connection(),
        write_fn=fake_write,
    )
    parsed = json.loads(received[0].decode("utf-8"))
    assert parsed["summary"]["status"] == "PASS"


# --- save_and_notify_quality_report ---


def _make_report(status="PASS"):
    return {
        "summary": {
            "contract_version": "v1",
            "pipeline": "gtfs",
            "execution_id": "exec-1",
            "status": status,
            "acceptance_rate": 1.0,
            "rows_failed": 0,
            "quality_report_path": "meta/quality-reports/gtfs/report.json",
            "failure_phase": None,
            "failure_message": None,
            "generated_at_utc": "2026-04-17T14:10:00+00:00",
        },
        "details": {},
    }


def _make_connection():
    return {"endpoint": "localhost", "access_key": "key", "secret_key": "secret"}


def test_save_and_notify_calls_write_fn_once():
    calls = []

    def fake_write(connection_data, buffer, bucket_name, object_name):
        calls.append((bucket_name, object_name))

    save_and_notify_quality_report(
        report=_make_report(),
        path="meta-bucket/quality-reports/gtfs/report.json",
        connection_data=_make_connection(),
        webhook_url="disabled",
        write_fn=fake_write,
    )
    assert len(calls) == 1


def test_save_and_notify_splits_path_into_bucket_and_object():
    calls = []

    def fake_write(connection_data, buffer, bucket_name, object_name):
        calls.append((bucket_name, object_name))

    save_and_notify_quality_report(
        report=_make_report(),
        path="my-bucket/folder/sub/report.json",
        connection_data=_make_connection(),
        webhook_url="disabled",
        write_fn=fake_write,
    )
    assert calls[0][0] == "my-bucket"
    assert calls[0][1] == "folder/sub/report.json"


def test_save_and_notify_write_fn_receives_valid_json():
    received = []

    def fake_write(connection_data, buffer, bucket_name, object_name):
        received.append(buffer)

    save_and_notify_quality_report(
        report=_make_report(),
        path="bucket/report.json",
        connection_data=_make_connection(),
        webhook_url="disabled",
        write_fn=fake_write,
    )
    parsed = json.loads(received[0].decode("utf-8"))
    assert parsed["summary"]["status"] == "PASS"


def test_save_and_notify_skips_webhook_when_disabled():
    calls = []

    def fake_write(connection_data, buffer, bucket_name, object_name):
        calls.append(True)

    save_and_notify_quality_report(
        report=_make_report(),
        path="bucket/report.json",
        connection_data=_make_connection(),
        webhook_url="disabled",
        write_fn=fake_write,
    )
    assert len(calls) == 1


def test_save_and_notify_swallows_webhook_failure(caplog):
    import logging

    def fake_write(connection_data, buffer, bucket_name, object_name):
        pass

    def failing_send_webhook(summary, webhook_url, **kwargs):
        raise RuntimeError("connection refused")

    import quality.reporting as reporting_module
    original = reporting_module.send_webhook
    reporting_module.send_webhook = failing_send_webhook
    try:
        with caplog.at_level(logging.WARNING, logger="quality.reporting"):
            save_and_notify_quality_report(
                report=_make_report(),
                path="bucket/report.json",
                connection_data=_make_connection(),
                webhook_url="http://fake-webhook/notify",
                write_fn=fake_write,
            )
        assert any("webhook" in msg.lower() for msg in caplog.messages)
    finally:
        reporting_module.send_webhook = original
