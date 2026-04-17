from gtfs.services.data_quality_report import create_failure_quality_report


def test_create_failure_quality_report_has_expected_fields():
    report = create_failure_quality_report(
        stage="extract_load_files",
        execution_id="exec-1",
        failure_phase="extract_load_files",
        failure_message="validation failed",
        validated_items_count=2,
        error_details={
            "errors_by_file": {
                "stops.txt": ["insufficient_lines:expected_at_least_2:found_1"]
            },
        },
        relocation_status="SUCCESS",
        relocation_error=None,
    )

    summary = report["summary"]
    details = report["details"]

    assert summary["pipeline"] == "gtfs"
    assert summary["stage"] == "extract_load_files"
    assert summary["status"] == "FAIL"
    assert summary["failure_phase"] == "extract_load_files"
    assert summary["rows_failed"] == 1
    assert summary["relocation_status"] == "SUCCESS"
    assert details["validated_items_count"] == 2
    assert "stops.txt" in details["error_details"]["errors_by_file"]
