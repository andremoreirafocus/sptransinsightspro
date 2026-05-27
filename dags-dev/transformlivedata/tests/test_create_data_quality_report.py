import json
import pandas as pd
import pytest
from transformlivedata.services.create_data_quality_report import (
    _compute_partial_metrics,
    _compute_quality_metrics,
    build_data_quality_report,
    build_quarantine_path,
    create_data_quality_metrics,
    create_data_quality_report,
    create_failure_quality_report,
)
from quality.reporting import build_quality_report_path, save_quality_report


@pytest.fixture
def shared_report_path_args(request):
    """Extracted args for calling shared build_quality_report_path()."""
    config = make_storage_config()
    return {
        "metadata_bucket": config["general"]["storage"]["metadata_bucket"],
        "quality_report_folder": config["general"]["storage"]["quality_report_folder"],
        "pipeline_name": "transformlivedata",
        "filename_label": "positions",
    }


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


def make_expectations_result(
    rows_failed=0, violations=0, exceptions=0, gx_invalid_count=0
):
    invalid_df = (
        pd.DataFrame({"id": range(gx_invalid_count)}) if gx_invalid_count > 0 else None
    )
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


def test_path_contains_metadata_bucket(shared_report_path_args):
    path = build_quality_report_path(**shared_report_path_args, batch_ts="2026-02-15T10:30:00")
    assert path.startswith("meta-bucket/")


def test_path_uses_hhmm_from_batch_ts(shared_report_path_args):
    path = build_quality_report_path(**shared_report_path_args, batch_ts="2026-02-15T10:30:00")
    assert "quality-report-positions_1030.json" in path


def test_path_partition_uses_date_components(shared_report_path_args):
    path = build_quality_report_path(**shared_report_path_args, batch_ts="2026-02-15T10:30:00")
    assert "year=2026" in path
    assert "month=02" in path
    assert "day=15" in path
    assert "hour=10" in path


# --- _compute_quality_metrics (status logic without DataFrames) ---


def test_compute_metrics_preserves_explicit_pass_status():
    result = _compute_quality_metrics(
        status="PASS",
        acceptance_rate=1.0,
        rows_failed=0,
    )
    assert result["status"] == "PASS"


def test_compute_metrics_preserves_explicit_fail_status():
    result = _compute_quality_metrics(
        status="FAIL",
    )
    assert result["status"] == "FAIL"


def test_compute_metrics_defaults_to_fail_when_failure_phase_set():
    result = _compute_quality_metrics(
        failure_phase="transform",
        failure_message="Something broke",
    )
    assert result["status"] == "FAIL"


def test_compute_metrics_defaults_to_warn_when_no_result_and_no_failure():
    result = _compute_quality_metrics()
    assert result["status"] == "WARN"


def test_summary_contains_required_keys():
    result = build_data_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="file.json",
        transform_result=make_transform_result(invalid_count=0, total=100),
        expectations_result=make_expectations_result(),
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
    )
    for key in (
        "contract_version",
        "pipeline",
        "execution_id",
        "status",
        "acceptance_rate",
        "items_failed",
    ):
        assert key in result["summary"]


# --- save_quality_report (shared) ---


def test_write_fn_receives_correct_bucket(shared_report_path_args):
    calls = []

    def fake_write(connection_data, buffer, bucket_name, object_name):
        calls.append(
            {"bucket_name": bucket_name, "object_name": object_name, "buffer": buffer}
        )

    config = make_storage_config()
    path = build_quality_report_path(**shared_report_path_args, batch_ts="2026-02-15T10:30:00")
    connection_data = {
        **config["connections"]["object_storage"],
        "secure": False,
    }
    save_quality_report(
        report={"key": "value"},
        path=path,
        connection_data=connection_data,
        write_fn=fake_write,
    )
    assert calls[0]["bucket_name"] == "meta-bucket"


def test_write_fn_object_name_contains_hhmm(shared_report_path_args):
    calls = []

    def fake_write(connection_data, buffer, bucket_name, object_name):
        calls.append(object_name)

    config = make_storage_config()
    path = build_quality_report_path(**shared_report_path_args, batch_ts="2026-02-15T10:30:00")
    connection_data = {
        **config["connections"]["object_storage"],
        "secure": False,
    }
    save_quality_report(
        report={"key": "value"},
        path=path,
        connection_data=connection_data,
        write_fn=fake_write,
    )
    assert "quality-report-positions_1030.json" in calls[0]


def test_write_fn_buffer_is_valid_json(shared_report_path_args):
    calls = []

    def fake_write(connection_data, buffer, bucket_name, object_name):
        calls.append(buffer)

    config = make_storage_config()
    path = build_quality_report_path(**shared_report_path_args, batch_ts="2026-02-15T10:30:00")
    connection_data = {
        **config["connections"]["object_storage"],
        "secure": False,
    }
    report = {"status": "PASS", "value": 42}
    save_quality_report(
        report=report,
        path=path,
        connection_data=connection_data,
        write_fn=fake_write,
    )
    parsed = json.loads(calls[0].decode("utf-8"))
    assert parsed["status"] == "PASS"


def test_write_fn_object_name_has_correct_partition_path(shared_report_path_args):
    calls = []

    def fake_write(connection_data, buffer, bucket_name, object_name):
        calls.append(object_name)

    config = make_storage_config()
    path = build_quality_report_path(**shared_report_path_args, batch_ts="2026-02-15T10:30:00")
    connection_data = {
        **config["connections"]["object_storage"],
        "secure": False,
    }
    save_quality_report(
        report={},
        path=path,
        connection_data=connection_data,
        write_fn=fake_write,
    )
    assert "year=2026/month=02/day=15/hour=10/" in calls[0]


# --- _compute_quality_metrics with transform_result + expectations_result ---


def test_compute_metrics_pass_with_clean_results():
    result = _compute_quality_metrics(
        transform_result=make_transform_result(invalid_count=0, total=100),
        expectations_result=make_expectations_result(),
        pass_threshold=1.0,
        warn_threshold=0.98,
    )
    assert result["status"] == "PASS"
    assert result["acceptance_rate"] == 1.0


def test_compute_metrics_warn_when_acceptance_between_thresholds():
    result = _compute_quality_metrics(
        transform_result=make_transform_result(invalid_count=1, total=100),
        expectations_result=make_expectations_result(),
        pass_threshold=1.0,
        warn_threshold=0.98,
    )
    assert result["status"] == "WARN"


def test_compute_metrics_fail_when_acceptance_below_warn_threshold():
    result = _compute_quality_metrics(
        transform_result=make_transform_result(invalid_count=10, total=100),
        expectations_result=make_expectations_result(),
        pass_threshold=1.0,
        warn_threshold=0.98,
    )
    assert result["status"] == "FAIL"


def test_compute_metrics_warn_when_violations_present():
    result = _compute_quality_metrics(
        transform_result=make_transform_result(invalid_count=0, total=100),
        expectations_result=make_expectations_result(violations=1),
        pass_threshold=1.0,
        warn_threshold=0.98,
    )
    assert result["status"] == "WARN"


def test_compute_metrics_rows_failed_includes_gx_invalid():
    result = _compute_quality_metrics(
        transform_result=make_transform_result(invalid_count=2, total=100),
        expectations_result=make_expectations_result(gx_invalid_count=3),
        pass_threshold=1.0,
        warn_threshold=0.98,
    )
    assert result["rows_failed"] == 5


def test_compute_metrics_acceptance_rate_zero_when_total_is_zero():
    result = _compute_quality_metrics(
        transform_result=make_transform_result(invalid_count=0, total=0),
        expectations_result=make_expectations_result(),
        pass_threshold=1.0,
        warn_threshold=0.98,
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


def test_create_data_quality_report_returns_log_metadata():
    result = create_data_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        transform_result=make_transform_result(),
        expectations_result=make_expectations_result(),
        write_fn=lambda *a, **kw: None,
    )
    assert "summary_status" in result
    assert "quality_report_path" in result
    assert "record_counts" in result


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


def test_create_data_quality_metrics_returns_expected_compact_counts():
    report = build_data_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        transform_result=make_transform_result(invalid_count=1, total=10),
        expectations_result=make_expectations_result(gx_invalid_count=2, rows_failed=3),
        pass_threshold=1.0,
        warn_threshold=0.98,
        batch_ts="2026-02-15T10:30:00",
    )
    metrics = create_data_quality_metrics(report)

    assert metrics["record_counts"]["raw_input_records"] == 10
    assert metrics["record_counts"]["transformed_records"] == 9
    assert metrics["record_counts"]["accepted_records"] == 10
    assert metrics["record_counts"]["rejected_records"] == 1
    assert metrics["transformation_processing_issues"]["invalid_trips_count"] == 0
    assert metrics["transformation_processing_issues"]["invalid_vehicle_ids_count"] == 0
    assert metrics["post_transformation_validation_summary"]["records_failed"] == report["summary"]["items_failed"]
    assert metrics["post_transformation_validation_summary"]["gx_records_failures"] == 3


# --- _compute_partial_metrics ---


def test_compute_partial_metrics_returns_empty_when_both_none():
    result = _compute_partial_metrics(None, None)
    assert result["record_counts"] == {}
    assert result["transformation_processing_metrics"] == {}
    assert result["transformation_processing_issues"] == {}
    assert result["post_transformation_validation_summary"] == {}


def test_compute_partial_metrics_extracts_transform_counts_when_only_transform_result():
    result = _compute_partial_metrics(make_transform_result(invalid_count=2, total=10), None)
    assert result["record_counts"]["transformed_records"] == 8
    assert result["record_counts"]["raw_input_records"] == 10
    assert result["record_counts"]["rejected_records"] == 2
    assert result["post_transformation_validation_summary"] == {}


def test_compute_partial_metrics_extracts_expectations_counts_when_only_expectations_result():
    result = _compute_partial_metrics(None, make_expectations_result())
    assert result["record_counts"]["accepted_records"] == 10
    assert result["transformation_processing_metrics"] == {}


def test_compute_partial_metrics_extracts_both_when_full_results():
    result = _compute_partial_metrics(
        make_transform_result(invalid_count=1, total=10),
        make_expectations_result(),
    )
    assert result["record_counts"]["raw_input_records"] == 10
    assert result["record_counts"]["accepted_records"] == 10
    assert result["post_transformation_validation_summary"]["total_checks"] == 5


# --- create_failure_quality_report ---


def test_create_failure_quality_report_returns_fail_status():
    result = create_failure_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        failure_phase="load",
        failure_message="File not found",
        write_fn=lambda *a, **kw: None,
    )
    assert result["summary_status"] == "FAIL"
    assert result["failure_phase"] == "load"
    assert result["failure_message"] == "File not found"


def test_create_failure_quality_report_without_results_returns_empty_metrics():
    result = create_failure_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        failure_phase="load",
        failure_message="File not found",
        write_fn=lambda *a, **kw: None,
    )
    assert result["record_counts"] == {}
    assert result["transformation_processing_metrics"] == {}


def test_create_failure_quality_report_with_results_returns_full_metrics():
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
    assert result["summary_status"] == "FAIL"
    assert "raw_input_records" in result["record_counts"]


def test_create_failure_quality_report_uses_partial_metrics_when_only_transform_available():
    result = create_failure_quality_report(
        config=make_storage_config(),
        execution_id="exec-1",
        logical_date_utc="2026-02-15T10:00:00+00:00",
        source_file="f.json",
        failure_phase="expectations_validation",
        failure_message="boom",
        transform_result=make_transform_result(invalid_count=1, total=100),
        write_fn=lambda *a, **kw: None,
    )
    assert result["record_counts"]["raw_input_records"] == 100
    assert result["record_counts"]["transformed_records"] == 99
    assert result["post_transformation_validation_summary"] == {}
    assert result["failure_phase"] == "expectations_validation"


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
        write_fn=lambda connection_data, buffer, bucket_name, object_name: calls.append(
            object_name
        ),
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


