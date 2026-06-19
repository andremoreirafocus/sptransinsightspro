import json
from datetime import datetime, timezone

from refinedtripfacts.services.create_quality_report import (
    create_failure_quality_report,
    create_final_quality_report,
)

RUN_TS = datetime(2026, 6, 8, 15, 0, 0, tzinfo=timezone.utc)
EXEC_ID = "aaaabbbb-cccc-dddd-eeee-ffff00001111"


def make_config():
    return {
        "general": {
            "storage": {
                "metadata_bucket": "metadata",
                "quality_report_folder": "quality-reports",
            },
        },
        "connections": {
            "object_storage": {
                "endpoint": "localhost:9000",
                "access_key": "minioadmin",
                "secret_key": "minioadmin",
            }
        },
    }


def make_quality_result(status="PASS", loss_rate=0.0, persisted=100, finished=100):
    return {
        "status": status,
        "loss_rate": loss_rate,
        "persisted_facts": persisted,
        "finished_trips_read": finished,
        "checks": [
            {"check": "completeness", "status": status},
            {"check": "dim_time_coverage", "status": "PASS", "uncovered_dim_keys": 0},
            {"check": "value_domain", "status": "PASS"},
        ],
    }


def make_measurement_result(finished_trips_read=100):
    return {"finished_trips_read": finished_trips_read}


def make_dim_time_result(rows_ensured=24, expected_count=48, existing_count=0):
    return {"rows_ensured": rows_ensured, "expected_count": expected_count, "existing_count": existing_count}


def make_creation_result(facts_derived=100, inserted_rows=100, skipped_rows=0):
    return {
        "facts_derived": facts_derived,
        "inserted_rows": inserted_rows,
        "skipped_rows": skipped_rows,
    }


def make_persisted_metrics(
    persisted_facts=100,
    uncovered_dim_keys=0,
    negative_duration=0,
    negative_distance=0,
    time_incoherent=0,
    implausible_speed=0,
):
    return {
        "persisted_facts": persisted_facts,
        "uncovered_dim_keys": uncovered_dim_keys,
        "negative_duration": negative_duration,
        "negative_distance": negative_distance,
        "time_incoherent": time_incoherent,
        "implausible_speed": implausible_speed,
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
# create_final_quality_report
# ---------------------------------------------------------------------------


def test_final_report_saves_to_minio():
    write = WriteCapture()
    create_final_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        measurement_result=make_measurement_result(),
        dim_time_result=make_dim_time_result(),
        creation_result=make_creation_result(),
        persisted_metrics=make_persisted_metrics(),
        quality_result=make_quality_result(),
        write_fn=write,
    )
    assert len(write.calls) == 1
    assert write.calls[0]["bucket_name"] == "metadata"
    assert "refinedtripfacts" in write.calls[0]["object_name"]


def test_final_report_path_contains_execution_id_prefix():
    write = WriteCapture()
    create_final_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        measurement_result=make_measurement_result(),
        dim_time_result=make_dim_time_result(),
        creation_result=make_creation_result(),
        persisted_metrics=make_persisted_metrics(),
        quality_result=make_quality_result(),
        write_fn=write,
    )
    assert "aaaabbbb" in write.calls[0]["object_name"]


def test_final_report_status_reflects_quality_result():
    write = WriteCapture()
    report = create_final_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        measurement_result=make_measurement_result(),
        dim_time_result=make_dim_time_result(),
        creation_result=make_creation_result(),
        persisted_metrics=make_persisted_metrics(),
        quality_result=make_quality_result(status="WARN", loss_rate=0.02),
        write_fn=write,
    )
    assert report["summary"]["status"] == "WARN"


def test_final_report_summary_contains_key_metrics():
    write = WriteCapture()
    report = create_final_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        measurement_result=make_measurement_result(finished_trips_read=200),
        dim_time_result=make_dim_time_result(),
        creation_result=make_creation_result(facts_derived=200, inserted_rows=195, skipped_rows=5),
        persisted_metrics=make_persisted_metrics(persisted_facts=195, uncovered_dim_keys=0),
        quality_result=make_quality_result(persisted=195, finished=200, loss_rate=0.025),
        write_fn=write,
    )
    summary = report["summary"]
    assert summary["finished_trips_read"] == 200
    assert summary["expected_count"] == 48
    assert summary["existing_count"] == 0
    assert summary["facts_derived"] == 200
    assert summary["inserted_rows"] == 195
    assert summary["skipped_rows"] == 5
    assert summary["persisted_facts"] == 195


def test_final_report_details_contain_all_phases():
    write = WriteCapture()
    report = create_final_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        measurement_result=make_measurement_result(),
        dim_time_result=make_dim_time_result(),
        creation_result=make_creation_result(),
        persisted_metrics=make_persisted_metrics(),
        quality_result=make_quality_result(),
        write_fn=write,
    )
    phases = report["details"]["phases"]
    assert "input_trips_measurement" in phases
    assert "dim_time_provisioning" in phases
    assert "trip_facts_creation" in phases
    assert "trip_facts_verification" in phases
    assert "data_quality_validation" in phases


def test_final_report_saved_json_matches_returned_report():
    write = WriteCapture()
    report = create_final_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        measurement_result=make_measurement_result(),
        dim_time_result=make_dim_time_result(),
        creation_result=make_creation_result(),
        persisted_metrics=make_persisted_metrics(),
        quality_result=make_quality_result(),
        write_fn=write,
    )
    saved = write.calls[0]["data"]
    assert saved["summary"]["execution_id"] == report["summary"]["execution_id"]
    assert saved["summary"]["status"] == report["summary"]["status"]


def test_final_report_includes_column_lineage():
    write = WriteCapture()
    lineage = {"table_name": "trip_facts", "drift_detected": False, "warning": None}
    report = create_final_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        measurement_result=make_measurement_result(),
        dim_time_result=make_dim_time_result(),
        creation_result=make_creation_result(),
        persisted_metrics=make_persisted_metrics(),
        quality_result=make_quality_result(),
        column_lineage=lineage,
        write_fn=write,
    )
    artifacts = report["details"]["artifacts"]
    assert artifacts["column_lineage"]["table_name"] == "trip_facts"
    assert artifacts["column_lineage"]["drift_detected"] is False


# ---------------------------------------------------------------------------
# create_failure_quality_report
# ---------------------------------------------------------------------------


def test_failure_report_saves_to_minio():
    write = WriteCapture()
    create_failure_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        failure_phase="trip_facts_creation",
        failure_message="insert failed",
        measurement_result=make_measurement_result(),
        write_fn=write,
    )
    assert len(write.calls) == 1
    assert write.calls[0]["bucket_name"] == "metadata"


def test_failure_report_exposes_failure_phase_and_message():
    write = WriteCapture()
    report = create_failure_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        failure_phase="trip_facts_creation",
        failure_message="insert failed",
        write_fn=write,
    )
    assert report["summary"]["status"] == "FAIL"
    assert report["summary"]["failure_phase"] == "trip_facts_creation"
    assert report["summary"]["failure_message"] == "insert failed"
    assert report["details"]["failure_phase"] == "trip_facts_creation"


def test_failure_report_preserves_partial_phase_results():
    write = WriteCapture()
    report = create_failure_quality_report(
        config=make_config(),
        execution_id=EXEC_ID,
        run_ts=RUN_TS,
        failure_phase="trip_facts_verification",
        failure_message="read-back failed",
        measurement_result=make_measurement_result(finished_trips_read=50),
        dim_time_result=make_dim_time_result(rows_ensured=24),
        creation_result=make_creation_result(facts_derived=50, inserted_rows=50, skipped_rows=0),
        write_fn=write,
    )
    summary = report["summary"]
    assert summary["expected_count"] == 48
    assert summary["existing_count"] == 0
    phases = report["details"]["phases"]
    assert phases["input_trips_measurement"]["finished_trips_read"] == 50
    assert phases["dim_time_provisioning"]["rows_ensured"] == 24
    assert phases["trip_facts_creation"]["facts_derived"] == 50
    assert "trip_facts_verification" not in phases
