import json
import os
import tempfile
import pandas as pd
import pytest
from transformlivedata.services.create_data_quality_report import (
    build_data_quality_report,
    build_quality_report_path,
    build_quality_summary,
    build_quarantine_path,
    create_data_quality_report,
    create_failure_quality_report,
    format_data_quality_report,
    save_data_quality_report_to_storage,
    write_data_quality_report_json,
)


def make_storage_config():
    return {
        "general": {
            "storage": {
                "metadata_bucket": "meta-bucket",
                "quality_report_folder": "quality",
                "app_folder": "app",
                "quarantined_bucket": "quarantine-bucket",
                "trusted_bucket": "trusted-bucket",
            },
            "tables": {
                "positions_table_name": "positions",
            },
        },
        "connections": {
            "object_storage": {
                "endpoint": "localhost",
                "access_key": "key",
                "secret_key": "secret",
            }
        },
    }


def make_transform_result(invalid_count=0, total=100):
    positions = pd.DataFrame({"id": range(total - invalid_count)})
    invalid_positions = pd.DataFrame({"id": range(invalid_count)})
    return {
        "positions": positions,
        "invalid_positions": invalid_positions,
        "metrics": {
            "total_vehicles_processed": total,
            "valid_vehicles": total - invalid_count,
            "invalid_vehicles": invalid_count,
            "expected_vehicles": total,
            "total_lines_processed": 5,
        },
        "issues": {
            "invalid_trips": [],
            "invalid_vehicle_ids": [],
            "distance_calculation_errors": [],
            "lines_with_invalid_vehicles": 0,
        },
        "batch_ts": "2026-02-15T10:30:00",
        "lineage": {},
    }


def make_expectations_result(rows_failed=0, violations=0, exceptions=0, gx_invalid_count=0):
    invalid_df = pd.DataFrame({"id": range(gx_invalid_count)}) if gx_invalid_count > 0 else None
    valid_df = pd.DataFrame({"id": range(10)})
    return {
        "valid_df": valid_df,
        "invalid_df": invalid_df,
        "expectations_summary": {
            "total_checks": 5,
            "expectations_successful": 5 - violations - exceptions,
            "expectations_with_violations": violations,
            "expectations_failed_due_to_exceptions": exceptions,
            "rows_failed": rows_failed,
            "violation_reasons": [],
            "exception_reasons": [],
        },
    }


# --- build_quality_report_path ---

def test_path_contains_metadata_bucket():
    config = make_storage_config()
    path = build_quality_report_path(config, "2026-02-15T10:30:00")
    assert path.startswith("meta-bucket/")


def test_path_uses_hhmm_from_batch_ts():
    config = make_storage_config()
    path = build_quality_report_path(config, "2026-02-15T10:30:00")
    assert "quality-report-positions_1030.json" in path


def test_path_partition_uses_date_components():
    config = make_storage_config()
    path = build_quality_report_path(config, "2026-02-15T10:30:00")
    assert "year=2026" in path
    assert "month=02" in path
    assert "day=15" in path
    assert "hour=10" in path


# --- build_quality_summary (status logic without DataFrames) ---

def test_summary_preserves_explicit_pass_status():
    config = make_storage_config()
    result = build_quality_summary(
        config=config,
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="file.json.zst",
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
        status="PASS",
        acceptance_rate=1.0,
        rows_failed=0,
    )
    assert result["status"] == "PASS"


def test_summary_preserves_explicit_fail_status():
    config = make_storage_config()
    result = build_quality_summary(
        config=config,
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="file.json.zst",
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
        status="FAIL",
    )
    assert result["status"] == "FAIL"


def test_summary_defaults_to_fail_when_failure_phase_set():
    config = make_storage_config()
    result = build_quality_summary(
        config=config,
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="file.json.zst",
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
        failure_phase="transform",
        failure_message="Something broke",
    )
    assert result["status"] == "FAIL"


def test_summary_defaults_to_warn_when_no_result_and_no_failure():
    config = make_storage_config()
    result = build_quality_summary(
        config=config,
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="file.json.zst",
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
    )
    assert result["status"] == "WARN"


def test_summary_contains_required_keys():
    config = make_storage_config()
    result = build_quality_summary(
        config=config,
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="file.json.zst",
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
        status="PASS",
    )
    for key in ("contract_version", "pipeline", "execution_id", "status", "acceptance_rate", "rows_failed"):
        assert key in result


# --- save_data_quality_report_to_storage ---

def test_write_fn_receives_correct_bucket():
    calls = []

    def fake_write(connection_data, buffer, bucket_name, object_name):
        calls.append({"bucket_name": bucket_name, "object_name": object_name, "buffer": buffer})

    save_data_quality_report_to_storage(
        make_storage_config(),
        {"key": "value"},
        "2026-02-15T10:30:00",
        write_fn=fake_write,
    )
    assert calls[0]["bucket_name"] == "meta-bucket"


def test_write_fn_object_name_contains_hhmm():
    calls = []

    def fake_write(connection_data, buffer, bucket_name, object_name):
        calls.append(object_name)

    save_data_quality_report_to_storage(
        make_storage_config(),
        {"key": "value"},
        "2026-02-15T10:30:00",
        write_fn=fake_write,
    )
    assert "quality-report-positions_1030.json" in calls[0]


def test_write_fn_buffer_is_valid_json():
    calls = []

    def fake_write(connection_data, buffer, bucket_name, object_name):
        calls.append(buffer)

    report = {"status": "PASS", "value": 42}
    save_data_quality_report_to_storage(
        make_storage_config(),
        report,
        "2026-02-15T10:30:00",
        write_fn=fake_write,
    )
    parsed = json.loads(calls[0].decode("utf-8"))
    assert parsed["status"] == "PASS"


def test_write_fn_object_name_has_correct_partition_path():
    calls = []

    def fake_write(connection_data, buffer, bucket_name, object_name):
        calls.append(object_name)

    save_data_quality_report_to_storage(
        make_storage_config(),
        {},
        "2026-02-15T10:30:00",
        write_fn=fake_write,
    )
    assert "year=2026/month=02/day=15/hour=10/" in calls[0]


# --- build_quality_summary with transform_result + expectations_result ---

def test_summary_computes_pass_with_clean_results():
    result = build_quality_summary(
        config=make_storage_config(),
        execution_id="x",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
        transform_result=make_transform_result(invalid_count=0, total=100),
        expectations_result=make_expectations_result(),
    )
    assert result["status"] == "PASS"
    assert result["acceptance_rate"] == 1.0


def test_summary_computes_warn_when_acceptance_between_thresholds():
    result = build_quality_summary(
        config=make_storage_config(),
        execution_id="x",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
        transform_result=make_transform_result(invalid_count=1, total=100),
        expectations_result=make_expectations_result(),
    )
    assert result["status"] == "WARN"


def test_summary_computes_fail_when_acceptance_below_warn_threshold():
    result = build_quality_summary(
        config=make_storage_config(),
        execution_id="x",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
        transform_result=make_transform_result(invalid_count=10, total=100),
        expectations_result=make_expectations_result(),
    )
    assert result["status"] == "FAIL"


def test_summary_computes_warn_when_violations_present():
    result = build_quality_summary(
        config=make_storage_config(),
        execution_id="x",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
        transform_result=make_transform_result(invalid_count=0, total=100),
        expectations_result=make_expectations_result(violations=1),
    )
    assert result["status"] == "WARN"


def test_summary_rows_failed_includes_gx_invalid():
    result = build_quality_summary(
        config=make_storage_config(),
        execution_id="x",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
        transform_result=make_transform_result(invalid_count=2, total=100),
        expectations_result=make_expectations_result(gx_invalid_count=3),
    )
    assert result["rows_failed"] == 5


def test_summary_acceptance_rate_zero_when_total_is_zero():
    result = build_quality_summary(
        config=make_storage_config(),
        execution_id="x",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
        transform_result=make_transform_result(invalid_count=0, total=0),
        expectations_result=make_expectations_result(),
    )
    assert result["acceptance_rate"] == 0.0


# --- build_data_quality_report ---

def test_build_data_quality_report_returns_summary_and_details():
    result = build_data_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        transform_result=make_transform_result(),
        expectations_result=make_expectations_result(),
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
    )
    assert "summary" in result
    assert "details" in result


def test_build_data_quality_report_status_pass():
    result = build_data_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        transform_result=make_transform_result(invalid_count=0, total=100),
        expectations_result=make_expectations_result(),
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
    )
    assert result["summary"]["status"] == "PASS"


def test_build_data_quality_report_status_warn():
    result = build_data_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        transform_result=make_transform_result(invalid_count=1, total=100),
        expectations_result=make_expectations_result(),
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
    )
    assert result["summary"]["status"] == "WARN"


def test_build_data_quality_report_status_fail():
    result = build_data_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        transform_result=make_transform_result(invalid_count=10, total=100),
        expectations_result=make_expectations_result(),
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
    )
    assert result["summary"]["status"] == "FAIL"


def test_build_data_quality_report_raises_on_missing_expectations_keys():
    with pytest.raises(ValueError, match="Error parsing expectations_result"):
        build_data_quality_report(
            config=make_storage_config(),
            execution_id="exec-1",
            logical_date_utc="2026-02-15T10:00:00+00:00",
            source_file="f.json",
            transform_result=make_transform_result(),
            expectations_result={},
            pass_threshold=1.0,
            warn_threshold=0.98,
            batch_ts="2026-02-15T10:30:00",
        )


def test_build_data_quality_report_zero_total_gives_zero_acceptance_rate():
    result = build_data_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        transform_result=make_transform_result(invalid_count=0, total=0),
        expectations_result=make_expectations_result(),
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
    )
    assert result["summary"]["acceptance_rate"] == 0.0


# --- create_data_quality_report ---

def test_create_data_quality_report_returns_report():
    result = create_data_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        transform_result=make_transform_result(),
        expectations_result=make_expectations_result(),
        write_fn=lambda *a, **kw: None,
    )
    assert "summary" in result
    assert "details" in result


def test_create_data_quality_report_calls_write_fn():
    calls = []
    create_data_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        transform_result=make_transform_result(),
        expectations_result=make_expectations_result(),
        write_fn=lambda *a, **kw: calls.append(True),
    )
    assert len(calls) == 1


# --- create_failure_quality_report ---

def test_create_failure_quality_report_status_is_fail():
    result = create_failure_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        failure_phase="load",
        failure_message="File not found",
        write_fn=lambda *a, **kw: None,
    )
    assert result["summary"]["status"] == "FAIL"


def test_create_failure_quality_report_without_results_has_minimal_details():
    result = create_failure_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        failure_phase="load",
        failure_message="File not found",
        write_fn=lambda *a, **kw: None,
    )
    assert result["details"]["transformation_row_counts"] == {}
    assert result["details"]["outcome"]["status"] == "FAIL"


def test_create_failure_quality_report_with_results_uses_full_report():
    result = create_failure_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        failure_phase="expectations",
        failure_message="Check failed",
        transform_result=make_transform_result(),
        expectations_result=make_expectations_result(),
        write_fn=lambda *a, **kw: None,
    )
    assert "transformation_row_counts" in result["details"]
    assert result["summary"]["status"] == "FAIL"


def test_create_failure_quality_report_uses_logical_date_when_batch_ts_none():
    calls = []
    create_failure_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        failure_phase="load",
        failure_message="err",
        batch_ts=None,
        write_fn=lambda connection_data, buffer, bucket_name, object_name: calls.append(object_name),
    )
    assert len(calls) == 1


def test_create_failure_quality_report_calls_write_fn():
    calls = []
    create_failure_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        failure_phase="load",
        failure_message="err",
        write_fn=lambda *a, **kw: calls.append(True),
    )
    assert len(calls) == 1


# --- build_quarantine_path ---

def test_quarantine_path_contains_quarantined_bucket():
    path = build_quarantine_path(make_storage_config(), "2026-02-15T10:30:00")
    assert path.startswith("quarantine-bucket/")


def test_quarantine_path_contains_table_name_and_partition():
    path = build_quarantine_path(make_storage_config(), "2026-02-15T10:30:00")
    assert "positions" in path
    assert "year=2026/month=02/day=15/hour=10/" in path


def test_quarantine_path_ends_with_parquet_glob():
    path = build_quarantine_path(make_storage_config(), "2026-02-15T10:30:00")
    assert path.endswith("*.parquet")


# --- format_data_quality_report ---

def _make_full_report(status="PASS"):
    return {
        "summary": {
            "execution_id": "exec-1",
            "logical_date_utc": "2026-02-15T10:00:00+00:00",
            "source_file": "f.json",
            "status": status,
            "acceptance_rate": 1.0,
            "rows_failed": 0,
        },
        "details": {
            "execution_id": "exec-1",
            "logical_date_utc": "2026-02-15T10:00:00+00:00",
            "source_file": "f.json",
            "transformation_row_counts": {"raw_records": 100, "transformed_records": 100, "accepted_records": 100, "rejected_records": 0},
            "transformation_metrics": {"total_vehicles_processed": 100, "valid_vehicles": 100, "invalid_vehicles": 0, "expected_vehicles": 100, "total_lines_processed": 5},
            "transformation_issues": {"invalid_trips": [], "invalid_vehicle_ids": [], "distance_calculation_errors": 0, "lines_with_invalid_vehicles": 0},
            "expectations_summary": {"total_checks": 5, "expectations_successful": 5, "expectations_with_violations": 0, "expectations_failed_due_to_exceptions": 0, "rows_failed": 0, "violation_reasons": [], "exception_reasons": []},
            "outcome": {"status": status, "acceptance_rate": 1.0, "policy_version": "v1"},
            "artifacts": {"quality_report_path": "bucket/path.json", "quarantine_path": "bucket/q/", "colum lineage": {}},
        },
    }


def test_format_report_contains_execution_id():
    output = format_data_quality_report(_make_full_report())
    assert "exec-1" in output


def test_format_report_contains_status():
    output = format_data_quality_report(_make_full_report(status="PASS"))
    assert "PASS" in output


def test_format_report_includes_violation_reasons_when_present():
    report = _make_full_report()
    report["details"]["expectations_summary"]["violation_reasons"] = ["col_a is null"]
    output = format_data_quality_report(report)
    assert "col_a is null" in output


def test_format_report_includes_exception_reasons_when_present():
    report = _make_full_report()
    report["details"]["expectations_summary"]["exception_reasons"] = ["timeout"]
    output = format_data_quality_report(report)
    assert "timeout" in output


def test_format_report_includes_expectations_failed_due_to_exceptions_line():
    report = _make_full_report()
    report["details"]["expectations_summary"]["expectations_failed_due_to_exceptions"] = 2
    output = format_data_quality_report(report)
    assert "Expectations failed due to exceptions: 2" in output


def test_format_report_raises_on_malformed_input():
    with pytest.raises(ValueError, match="Error parsing data_quality_report"):
        format_data_quality_report({"details": {"outcome": {"acceptance_rate": "not_a_number"}}})


# --- write_data_quality_report_json ---

def test_write_data_quality_report_json_writes_valid_json():
    report = {"status": "PASS", "count": 42}
    with tempfile.NamedTemporaryFile(mode="r", suffix=".json", delete=False) as f:
        path = f.name
    try:
        write_data_quality_report_json(report, path)
        with open(path) as f:
            parsed = json.load(f)
        assert parsed["status"] == "PASS"
        assert parsed["count"] == 42
    finally:
        os.unlink(path)
