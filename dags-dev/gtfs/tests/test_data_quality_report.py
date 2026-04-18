from quality.reporting import build_quality_report_path, save_quality_report
from gtfs.services.create_data_quality_report import (
    build_data_quality_report,
    create_data_quality_report,
    create_failure_quality_report,
)


def _config():
    return {
        "general": {
            "storage": {
                "metadata_bucket": "metadata",
                "quality_report_folder": "quality-reports",
            }
        },
        "connections": {
            "object_storage": {
                "endpoint": "localhost",
                "access_key": "key",
                "secret_key": "secret",
            }
        },
    }


def _stage_results_fail():
    return {
        "extract_load_files": {
            "status": "PASS",
            "validated_items_count": 10,
            "error_details": {"errors_by_file": {}},
            "relocation_status": "NOT_APPLICABLE",
            "relocation_error": None,
        },
        "transformation": {
            "status": "FAIL",
            "validated_items_count": 6,
            "error_details": {
                "errors_by_table": {"stop_times": ["gx_validation_exception"]},
            },
            "relocation_status": "SUCCESS",
            "relocation_error": None,
        },
    }


def test_build_quality_report_path_uses_execution_based_name():
    path = build_quality_report_path(
        metadata_bucket="metadata",
        quality_report_folder="quality-reports",
        pipeline_name="gtfs",
        batch_ts="2026-04-17T14:10:00",
        filename_suffix="_exec1234",
    )
    assert path.startswith("metadata/quality-reports/gtfs/year=2026/")
    assert "month=04/day=17/hour=14/" in path
    assert path.endswith("quality-report-gtfs_1410_exec1234.json")


def test_save_quality_report_calls_write_fn_with_correct_bucket_and_object():
    calls = []

    def fake_write(connection_data, buffer, bucket_name, object_name):
        calls.append((bucket_name, object_name))

    path = (
        "metadata/quality-reports/gtfs/year=2026/month=04/day=17/hour=14/"
        "quality-report-gtfs_1410_exec1234.json"
    )
    save_quality_report(
        report={"summary": {"status": "PASS"}, "details": {}},
        path=path,
        connection_data={"endpoint": "localhost", "access_key": "k", "secret_key": "s"},
        write_fn=fake_write,
    )
    assert calls[0][0] == "metadata"
    assert calls[0][1] == (
        "quality-reports/gtfs/year=2026/month=04/day=17/hour=14/"
        "quality-report-gtfs_1410_exec1234.json"
    )


def test_create_failure_quality_report_has_consolidated_stage_details():
    calls = []

    def fake_write(connection_data, buffer, bucket_name, object_name):
        calls.append((bucket_name, object_name))

    report = create_failure_quality_report(
        config=_config(),
        execution_id="exec-2",
        failure_phase="transformation",
        failure_message="Validation failures detected",
        stage_results=_stage_results_fail(),
        batch_ts="2026-04-17T14:10:00",
        write_fn=fake_write,
    )

    summary = report["summary"]
    details = report["details"]

    assert summary["pipeline"] == "gtfs"
    assert summary["stage"] == "pipeline"
    assert summary["status"] == "FAIL"
    assert summary["failure_phase"] == "transformation"
    assert details["stages"]["transformation"]["status"] == "FAIL"
    assert len(calls) == 1


def test_create_data_quality_report_persists_single_consolidated_report():
    calls = []
    stage_results = {
        "extract_load_files": {
            "status": "PASS",
            "validated_items_count": 10,
            "error_details": {"errors_by_file": {}},
            "relocation_status": "NOT_APPLICABLE",
            "relocation_error": None,
        },
        "transformation": {
            "status": "PASS",
            "validated_items_count": 6,
            "error_details": {"errors_by_table": {}},
            "relocation_status": "SUCCESS",
            "relocation_error": None,
        },
    }

    def fake_write(connection_data, buffer, bucket_name, object_name):
        calls.append((bucket_name, object_name))

    report = create_data_quality_report(
        config=_config(),
        execution_id="exec-3",
        stage_results=stage_results,
        batch_ts="2026-04-17T14:10:00",
        write_fn=fake_write,
    )

    assert report["summary"]["status"] == "PASS"
    assert report["summary"]["validated_items_count"] == 16
    assert report["details"]["stages"]["extract_load_files"]["status"] == "PASS"
    assert len(calls) == 1


def test_create_data_quality_report_acceptance_rate_is_continuous():
    stage_results = {
        "extract_load_files": {
            "status": "PASS",
            "validated_items_count": 10,
            "error_details": {"errors_by_file": {"bad_file.txt": {}}},
            "relocation_status": "NOT_APPLICABLE",
            "relocation_error": None,
        },
        "transformation": {
            "status": "PASS",
            "validated_items_count": 10,
            "error_details": {"errors_by_table": {}},
            "relocation_status": "SUCCESS",
            "relocation_error": None,
        },
    }

    report = create_data_quality_report(
        config=_config(),
        execution_id="exec-4",
        stage_results=stage_results,
        batch_ts="2026-04-17T14:10:00",
        write_fn=lambda *a, **kw: None,
    )

    assert 0.0 < report["summary"]["acceptance_rate"] < 1.0


def test_create_data_quality_report_acceptance_rate_is_one_when_no_failures():
    stage_results = {
        "extract_load_files": {
            "status": "PASS",
            "validated_items_count": 10,
            "error_details": {"errors_by_file": {}},
            "relocation_status": "NOT_APPLICABLE",
            "relocation_error": None,
        },
    }

    report = create_data_quality_report(
        config=_config(),
        execution_id="exec-5",
        stage_results=stage_results,
        batch_ts="2026-04-17T14:10:00",
        write_fn=lambda *a, **kw: None,
    )

    assert report["summary"]["acceptance_rate"] == 1.0
