from src.quality.reporting import (
    build_execution_report_metadata,
    build_quality_summary,
    create_failure_quality_report,
)


def test_build_quality_summary_contract_shape():
    summary = build_quality_summary(
        pipeline="extractloadlivedata",
        execution_id="exec-1",
        status="PASS",
        items_failed=0,
        quality_report_path="null",
        acceptance_rate=1.0,
        custom_field="custom-value",
    )
    assert summary["contract_version"] == "v1"
    assert summary["pipeline"] == "extractloadlivedata"
    assert summary["execution_id"] == "exec-1"
    assert summary["status"] == "PASS"
    assert summary["items_failed"] == 0
    assert summary["quality_report_path"] == "null"
    assert summary["acceptance_rate"] == 1.0
    assert summary["custom_field"] == "custom-value"
    assert "generated_at_utc" in summary


def test_create_failure_quality_report_returns_summary_only():
    report = create_failure_quality_report(
        pipeline="extractloadlivedata",
        execution_id="exec-2",
        failure_phase="positions_download",
        failure_message="[SEVERE] non recoverable api get failed",
        quality_report_path="null",
        details={"ignored": True},
        retries=3,
    )
    assert report["status"] == "FAIL"
    assert report["pipeline"] == "extractloadlivedata"
    assert report["execution_id"] == "exec-2"
    assert report["failure_phase"] == "positions_download"
    assert (
        report["failure_message"] == "[SEVERE] non recoverable api get failed"
    )
    assert report["quality_report_path"] == "null"
    assert report["items_failed"] == 1
    assert report["acceptance_rate"] == 1.0
    assert report["retries"] == 3
    assert "details" not in report


def test_build_execution_report_metadata_includes_required_fields():
    metadata = build_execution_report_metadata(
        execution_seconds=12.5,
        items_total=10,
        items_failed=1,
        retries_seen=2,
        worked_correlation_ids=["2026-05-10T12:00:00Z"],
    )
    assert metadata["execution_seconds"] == 12.5
    assert metadata["items_total"] == 10
    assert metadata["items_failed"] == 1
    assert metadata["retries_seen"] == 2
    assert metadata["correlation_ids"] == ["2026-05-10T12:00:00Z"]
    assert metadata["correlation_ids_count"] == 1
    assert metadata["correlation_ids_truncated"] is False


def test_build_execution_report_metadata_dedups_and_preserves_order():
    metadata = build_execution_report_metadata(
        execution_seconds=3.2,
        items_total=4,
        items_failed=0,
        retries_seen=0,
        worked_correlation_ids=[
            "2026-05-10T12:02:00Z",
            "2026-05-10T12:01:00Z",
            "2026-05-10T12:02:00Z",
            "2026-05-10T12:03:00Z",
            "2026-05-10T12:01:00Z",
        ],
    )
    assert metadata["correlation_ids"] == [
        "2026-05-10T12:02:00Z",
        "2026-05-10T12:01:00Z",
        "2026-05-10T12:03:00Z",
    ]
    assert metadata["correlation_ids_count"] == 3
    assert metadata["correlation_ids_truncated"] is False


def test_build_execution_report_metadata_applies_truncation_with_count():
    worked_correlation_ids = [f"corr-{i:03d}" for i in range(60)]
    metadata = build_execution_report_metadata(
        execution_seconds=9.0,
        items_total=60,
        items_failed=0,
        retries_seen=0,
        worked_correlation_ids=worked_correlation_ids,
    )
    assert len(metadata["correlation_ids"]) == 50
    assert metadata["correlation_ids"][0] == "corr-000"
    assert metadata["correlation_ids"][-1] == "corr-049"
    assert metadata["correlation_ids_count"] == 60
    assert metadata["correlation_ids_truncated"] is True
