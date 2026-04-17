from gtfs.services.data_quality_report import create_failure_quality_report


def test_create_failure_quality_report_has_expected_fields():
    report = create_failure_quality_report(
        execution_id="exec-1",
        failure_phase="extract_load_files",
        failure_message="validation failed",
        validation_result={
            "validated_files_count": 2,
            "errors_by_file": {
                "stops.txt": ["insufficient_lines:expected_at_least_2:found_1"]
            },
        },
        quarantine_save_status="SUCCESS",
        quarantine_save_error=None,
    )

    summary = report["summary"]
    details = report["details"]

    assert summary["pipeline"] == "gtfs"
    assert summary["status"] == "FAIL"
    assert summary["failure_phase"] == "extract_load_files"
    assert summary["rows_failed"] == 1
    assert summary["quarantine_save_status"] == "SUCCESS"
    assert details["validated_files_count"] == 2
    assert "stops.txt" in details["errors_by_file"]
