import json
from datetime import datetime, timezone

from refinedfinishedtrips.services.create_quality_report import (
    build_quality_report,
    create_failure_quality_report,
    create_final_quality_report,
    create_quality_report,
)

RUN_TS = datetime(2026, 4, 27, 14, 0, 0, tzinfo=timezone.utc)
EXEC_ID = "aaaabbbb-cccc-dddd-eeee-ffff00001111"


def make_config():
    return {
        "general": {
            "storage": {
                "metadata_bucket": "metadata",
                "quality_report_folder": "quality-reports",
            },
            "notifications": {"webhook_url": "disabled"},
        },
        "connections": {
            "object_storage": {
                "endpoint": "localhost:9000",
                "access_key": "minioadmin",
                "secret_key": "minioadmin",
            }
        },
    }


def make_positions_result(status="PASS", checks=None, count=100):
    checks = checks or []
    return {
        "status": status,
        "positions_in_time_window_count": count,
        "checks": checks,
    }


class WriteCapture:
    def __init__(self):
        self.calls = []

    def __call__(self, connection_data, buffer, bucket_name, object_name):
        self.calls.append(
            {
                "bucket_name": bucket_name,
                "object_name": object_name,
                "data": json.loads(buffer),
            }
        )


# ---------------------------------------------------------------------------
# build_quality_report
# ---------------------------------------------------------------------------


def test_build_quality_report_structure():
    path = "metadata/quality-reports/refinedfinishedtrips/year=2026/month=04/day=27/hour=14/quality-report-refinedfinishedtrips_1400_aaaabbbb.json"
    result = build_quality_report(
        execution_id=EXEC_ID,
        positions_result=make_positions_result(),
        quality_report_path=path,
        status="PASS",
    )
    assert "summary" in result
    assert "details" in result
    summary = result["summary"]
    assert summary["pipeline"] == "refinedfinishedtrips"
    assert summary["execution_id"] == EXEC_ID
    assert summary["status"] == "PASS"
    assert summary["items_failed"] == 0
    assert summary["quality_report_path"] == path
    assert summary["positions_in_time_window_count"] == 100
    assert summary["failure_phase"] is None
    assert summary["failure_message"] is None


def test_build_quality_report_details_structure():
    path = "metadata/some-path.json"
    result = build_quality_report(
        execution_id=EXEC_ID,
        positions_result=make_positions_result(),
        quality_report_path=path,
        status="PASS",
    )
    details = result["details"]
    assert details["execution_id"] == EXEC_ID
    assert details["status"] == "PASS"
    assert "positions" in details["phases"]
    assert details["artifacts"]["quality_report_path"] == path


def test_build_quality_report_items_failed_counts_only_fail_checks():
    checks = [
        {"check": "freshness", "status": "FAIL"},
        {"check": "recent_gaps", "status": "WARN"},
    ]
    positions_result = make_positions_result(status="FAIL", checks=checks)
    result = build_quality_report(
        execution_id=EXEC_ID,
        positions_result=positions_result,
        quality_report_path="metadata/p.json",
        status="FAIL",
    )
    assert result["summary"]["items_failed"] == 1


def test_build_quality_report_items_failed_zero_when_only_warn():
    checks = [
        {"check": "freshness", "status": "WARN"},
        {"check": "recent_gaps", "status": "PASS"},
    ]
    positions_result = make_positions_result(status="WARN", checks=checks)
    result = build_quality_report(
        execution_id=EXEC_ID,
        positions_result=positions_result,
        quality_report_path="metadata/p.json",
        status="WARN",
    )
    assert result["summary"]["items_failed"] == 0


def test_build_quality_report_failure_fields_propagated():
    result = build_quality_report(
        execution_id=EXEC_ID,
        positions_result=make_positions_result(status="FAIL"),
        quality_report_path="metadata/p.json",
        status="FAIL",
        failure_phase="positions",
        failure_message="freshness check failed",
    )
    assert result["summary"]["failure_phase"] == "positions"
    assert result["summary"]["failure_message"] == "freshness check failed"
    assert result["details"]["failure_phase"] == "positions"


def test_build_quality_report_includes_partial_phase_results_when_provided():
    result = build_quality_report(
        execution_id=EXEC_ID,
        positions_result=make_positions_result(status="PASS"),
        quality_report_path="metadata/p.json",
        status="FAIL",
        failure_phase="persistence",
        failure_message="save failed",
        trips_result={"status": "PASS", "checks": []},
        persistence_result={
            "status": "FAIL",
            "added_rows": 0,
            "previously_saved_rows": 0,
        },
    )
    phases = result["details"]["phases"]
    assert phases["positions"]["status"] == "PASS"
    assert phases["trip_extraction"]["status"] == "PASS"
    assert phases["persistence"]["status"] == "FAIL"


def test_build_quality_report_includes_column_lineage_when_provided():
    result = build_quality_report(
        execution_id=EXEC_ID,
        positions_result=make_positions_result(status="PASS"),
        quality_report_path="metadata/p.json",
        status="FAIL",
        failure_phase="persistence",
        failure_message="save failed",
        column_lineage={"table_name": "finished_trips", "columns": {"trip_id": {}}},
    )
    artifacts = result["details"]["artifacts"]
    assert artifacts["column_lineage"]["table_name"] == "finished_trips"


# ---------------------------------------------------------------------------
# create_failure_quality_report
# ---------------------------------------------------------------------------


def test_create_failure_quality_report_saves_to_minio():
    write = WriteCapture()
    checks = [{"check": "freshness", "status": "FAIL", "note": "no positions"}]
    positions_result = make_positions_result(status="FAIL", checks=checks, count=0)

    create_failure_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        failure_phase="positions",
        failure_message="no positions available for the analysis time window",
        positions_result=positions_result,
        write_fn=write,
    )

    assert len(write.calls) == 1
    call = write.calls[0]
    assert call["bucket_name"] == "metadata"
    assert "refinedfinishedtrips" in call["object_name"]
    assert "quality-report" in call["object_name"]


def test_create_failure_quality_report_path_contains_execution_id_prefix():
    write = WriteCapture()
    create_failure_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        failure_phase="positions",
        failure_message="no positions",
        positions_result=make_positions_result(status="FAIL"),
        write_fn=write,
    )
    object_name = write.calls[0]["object_name"]
    # execution_id prefix: first 8 hex chars without dashes = "aaaabbbb"
    assert "aaaabbbb" in object_name


def test_create_failure_quality_report_returns_report_with_status_fail():
    write = WriteCapture()
    report = create_failure_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        failure_phase="positions",
        failure_message="no positions",
        positions_result=make_positions_result(status="FAIL"),
        write_fn=write,
    )
    assert report["summary"]["status"] == "FAIL"
    assert report["summary"]["failure_phase"] == "positions"


def test_create_failure_quality_report_saved_json_matches_returned_report():
    write = WriteCapture()
    report = create_failure_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        failure_phase="positions",
        failure_message="no positions",
        positions_result=make_positions_result(status="FAIL"),
        write_fn=write,
    )
    saved = write.calls[0]["data"]
    assert saved["summary"]["execution_id"] == report["summary"]["execution_id"]
    assert saved["summary"]["status"] == "FAIL"


def test_create_failure_quality_report_preserves_partial_phase_results():
    write = WriteCapture()
    report = create_failure_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        failure_phase="persistence",
        failure_message="save failed",
        positions_result=make_positions_result(status="PASS"),
        trips_result={
            "status": "PASS",
            "effective_window_minutes": 180.0,
            "trips_extracted": 12,
            "checks": [],
        },
        persistence_result={
            "status": "FAIL",
            "added_rows": 0,
            "previously_saved_rows": 0,
        },
        write_fn=write,
    )
    phases = report["details"]["phases"]
    assert phases["positions"]["status"] == "PASS"
    assert phases["trip_extraction"]["trips_extracted"] == 12
    assert phases["persistence"]["status"] == "FAIL"


def test_create_failure_quality_report_includes_column_lineage_when_provided():
    write = WriteCapture()
    report = create_failure_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        failure_phase="persistence",
        failure_message="save failed",
        positions_result=make_positions_result(status="PASS"),
        trips_result={
            "status": "PASS",
            "effective_window_minutes": 180.0,
            "trips_extracted": 12,
            "checks": [],
        },
        persistence_result={
            "status": "FAIL",
            "added_rows": 0,
            "previously_saved_rows": 0,
        },
        column_lineage={"table_name": "finished_trips", "columns": {"trip_id": {}}},
        write_fn=write,
    )
    artifacts = report["details"]["artifacts"]
    assert artifacts["column_lineage"]["table_name"] == "finished_trips"


# ---------------------------------------------------------------------------
# create_quality_report (WARN / PASS path)
# ---------------------------------------------------------------------------


def test_create_quality_report_saves_warn_report_to_minio():
    write = WriteCapture()
    checks = [{"check": "freshness", "status": "WARN"}]
    positions_result = make_positions_result(status="WARN", checks=checks)

    report = create_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        positions_result=positions_result,
        write_fn=write,
    )

    assert len(write.calls) == 1
    assert write.calls[0]["bucket_name"] == "metadata"
    assert report["summary"]["status"] == "WARN"


def test_create_quality_report_status_derived_from_positions_result():
    write = WriteCapture()

    report_pass = create_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        positions_result=make_positions_result(status="PASS"),
        write_fn=write,
    )
    report_warn = create_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        positions_result=make_positions_result(status="WARN"),
        write_fn=write,
    )

    assert report_pass["summary"]["status"] == "PASS"
    assert report_warn["summary"]["status"] == "WARN"


def test_create_quality_report_path_contains_execution_id_prefix():
    write = WriteCapture()
    create_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        positions_result=make_positions_result(status="WARN"),
        write_fn=write,
    )
    assert "aaaabbbb" in write.calls[0]["object_name"]


# ---------------------------------------------------------------------------
# create_final_quality_report
# ---------------------------------------------------------------------------


def make_trips_result(
    status="PASS",
    trips_extracted=10,
    source_sentido_discrepancies=0,
    sanitization_dropped_points=0,
    vehicle_line_groups_processed=0,
):
    return {
        "status": status,
        "effective_window_minutes": 180.0,
        "trips_extracted": trips_extracted,
        "source_sentido_discrepancies": source_sentido_discrepancies,
        "sanitization_dropped_points": sanitization_dropped_points,
        "vehicle_line_groups_processed": vehicle_line_groups_processed,
        "checks": [],
    }


def make_persistence_result(
    status="PASS", added_rows=10, previously_saved_rows=0
):
    return {
        "status": status,
        "added_rows": added_rows,
        "previously_saved_rows": previously_saved_rows,
    }


def test_create_final_quality_report_saves_to_minio():
    write = WriteCapture()
    create_final_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        positions_result=make_positions_result(),
        trips_result=make_trips_result(),
        persistence_result=make_persistence_result(),
        write_fn=write,
    )
    assert len(write.calls) == 1
    assert write.calls[0]["bucket_name"] == "metadata"


def test_create_final_quality_report_overall_pass_when_all_pass():
    write = WriteCapture()
    report = create_final_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        positions_result=make_positions_result(status="PASS"),
        trips_result=make_trips_result(status="PASS"),
        persistence_result=make_persistence_result(status="PASS"),
        write_fn=write,
    )
    assert report["summary"]["status"] == "PASS"


def test_create_final_quality_report_overall_warn_when_any_warn():
    write = WriteCapture()
    report = create_final_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        positions_result=make_positions_result(status="PASS"),
        trips_result=make_trips_result(status="WARN"),
        persistence_result=make_persistence_result(status="PASS"),
        write_fn=write,
    )
    assert report["summary"]["status"] == "WARN"


def test_create_final_quality_report_summary_contains_all_metrics():
    write = WriteCapture()
    report = create_final_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        positions_result=make_positions_result(count=150000),
        trips_result=make_trips_result(
            trips_extracted=1247,
            source_sentido_discrepancies=13,
            sanitization_dropped_points=879,
            vehicle_line_groups_processed=8577,
        ),
        persistence_result=make_persistence_result(
            added_rows=245, previously_saved_rows=1002
        ),
        write_fn=write,
    )
    summary = report["summary"]
    assert summary["positions_in_time_window_count"] == 150000
    assert summary["trips_extracted"] == 1247
    assert summary["source_sentido_discrepancies"] == 13
    assert summary["sanitization_dropped_points"] == 879
    assert summary["vehicle_line_groups_processed"] == 8577
    assert summary["added_rows"] == 245
    assert summary["previously_saved_rows"] == 1002


def test_create_final_quality_report_details_contains_all_phases():
    write = WriteCapture()
    report = create_final_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        positions_result=make_positions_result(),
        trips_result=make_trips_result(),
        persistence_result=make_persistence_result(),
        write_fn=write,
    )
    phases = report["details"]["phases"]
    assert "positions" in phases
    assert "trip_extraction" in phases
    assert "persistence" in phases


def test_create_final_quality_report_details_do_not_include_execution_efficiency():
    write = WriteCapture()
    report = create_final_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        positions_result=make_positions_result(),
        trips_result=make_trips_result(),
        persistence_result=make_persistence_result(
            added_rows=0, previously_saved_rows=10
        ),
        write_fn=write,
    )
    assert "execution_efficiency" not in report["details"]


def test_create_final_quality_report_path_contains_execution_id_prefix():
    write = WriteCapture()
    create_final_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        positions_result=make_positions_result(),
        trips_result=make_trips_result(),
        persistence_result=make_persistence_result(),
        write_fn=write,
    )
    assert "aaaabbbb" in write.calls[0]["object_name"]


def test_create_final_quality_report_saved_json_matches_returned_report():
    write = WriteCapture()
    report = create_final_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        positions_result=make_positions_result(),
        trips_result=make_trips_result(),
        persistence_result=make_persistence_result(),
        write_fn=write,
    )
    saved = write.calls[0]["data"]
    assert saved["summary"]["execution_id"] == report["summary"]["execution_id"]
    assert saved["summary"]["status"] == report["summary"]["status"]


def test_create_final_quality_report_includes_column_lineage():
    write = WriteCapture()
    report = create_final_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        positions_result=make_positions_result(),
        trips_result=make_trips_result(),
        persistence_result=make_persistence_result(),
        column_lineage={
            "table_name": "finished_trips",
            "columns": {"trip_id": {}, "vehicle_id": {}},
            "drift_detected": False,
            "warning": None,
        },
        write_fn=write,
    )
    artifacts = report["details"]["artifacts"]
    assert artifacts["column_lineage"]["table_name"] == "finished_trips"
    assert artifacts["column_lineage"]["drift_detected"] is False
