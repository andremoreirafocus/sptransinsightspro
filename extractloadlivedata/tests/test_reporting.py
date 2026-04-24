from src.reporting import build_quality_summary, create_failure_quality_report


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
