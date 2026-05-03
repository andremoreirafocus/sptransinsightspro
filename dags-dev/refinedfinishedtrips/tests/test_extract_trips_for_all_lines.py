from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from refinedfinishedtrips.extract_trips_for_all_Lines_and_vehicles import (
    extract_trips_for_all_Lines_and_vehicles,
)

BASE_TS = datetime(2026, 4, 14, 10, 0, 0, tzinfo=timezone.utc)


def make_config(webhook_url="disabled"):
    return {
        "general": {
            "tables": {"finished_trips_table_name": "finished_trips"},
            "notifications": {"webhook_url": webhook_url},
            "quality": {
                "trips_effective_window_threshold_minutes": 60,
                "trips_min_trips_threshold": 5,
            },
            "trip_detection": {"stop_proximity_threshold_meters": 100},
        },
        "connections": {
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "testdb",
                "user": "user",
                "password": "pass",
            },
            "object_storage": {
                "endpoint": "localhost:9000",
                "access_key": "minioadmin",
                "secret_key": "minioadmin",
            },
        },
    }


def positions_pass_stub(config, df):
    return {
        "status": "PASS",
        "positions_in_time_window_count": len(df),
        "checks": [],
    }


def positions_warn_stub(config, df):
    return {
        "status": "WARN",
        "positions_in_time_window_count": len(df),
        "checks": [
            {
                "check": "freshness",
                "status": "WARN",
                "note": "freshness lag above warning threshold",
            }
        ],
    }


def positions_fail_stub(config, df):
    return {
        "status": "FAIL",
        "positions_in_time_window_count": len(df),
        "checks": [
            {
                "check": "freshness",
                "status": "FAIL",
                "note": "no positions available for the analysis time window",
            }
        ],
    }


def trips_pass_stub(config, df, trips, extraction_metrics=None):
    return {
        "status": "PASS",
        "effective_window_minutes": 0.0,
        "trips_extracted": len(trips),
        "source_sentido_discrepancies": (extraction_metrics or {}).get(
            "total_source_sentido_discrepancies", 0
        ),
        "sanitization_dropped_points": (extraction_metrics or {}).get(
            "total_input_position_sanitization_drops", 0
        ),
        "input_position_records": (extraction_metrics or {}).get("total_input_position_records", len(df)),
        "vehicle_line_groups_processed": (extraction_metrics or {}).get("vehicle_line_groups_processed", 0),
        "checks": [],
    }


def persistence_pass_stub(save_result):
    return {
        "status": "PASS",
        "new_rows": save_result.get("new_rows", 0),
        "skipped_rows": save_result.get("skipped_rows", 0),
    }


def noop_create_failure_report(
    config,
    execution_id,
    run_ts,
    failure_phase,
    failure_message,
    positions_result,
    trips_result=None,
    persistence_result=None,
    write_fn=None,
):
    return {"summary": {"status": "FAIL"}, "details": {}}


def noop_create_final_report(config, execution_id, run_ts, positions_result, trips_result, persistence_result, write_fn=None):
    statuses = [r["status"] for r in (positions_result, trips_result, persistence_result)]
    status = "WARN" if "WARN" in statuses else "PASS"
    return {"summary": {"status": status, "execution_id": execution_id}, "details": {}}


def noop_send_webhook(summary, webhook_url):
    pass


def noop_save_trips(config, trips):
    return {"new_rows": len(trips), "skipped_rows": 0}


class SaveCapture:
    def __init__(self):
        self.calls = []

    def __call__(self, config, trips):
        self.calls.append(trips)
        return {"new_rows": len(trips), "skipped_rows": 0}


def make_positions_df(rows):
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Positions FAIL → pipeline stops, phases 2 and 3 not executed
# ---------------------------------------------------------------------------


def test_positions_fail_raises_and_save_not_called():
    save = SaveCapture()
    with pytest.raises(ValueError, match="Positions quality check FAILED"):
        extract_trips_for_all_Lines_and_vehicles(
            make_config(),
            get_recent_positions_fn=lambda c: pd.DataFrame(),
            save_trips_fn=save,
            validate_positions_fn=positions_fail_stub,
            create_failure_report_fn=noop_create_failure_report,
            send_webhook_fn=noop_send_webhook,
        )
    assert save.calls == []


def test_positions_fail_calls_create_failure_report():
    captured = []

    def capturing_create_failure_report(
        config,
        execution_id,
        run_ts,
        failure_phase,
        failure_message,
        positions_result,
        trips_result=None,
        persistence_result=None,
        write_fn=None,
    ):
        captured.append({"failure_phase": failure_phase, "failure_message": failure_message})
        return {"summary": {"status": "FAIL"}, "details": {}}

    with pytest.raises(ValueError):
        extract_trips_for_all_Lines_and_vehicles(
            make_config(),
            get_recent_positions_fn=lambda c: pd.DataFrame(),
            validate_positions_fn=positions_fail_stub,
            create_failure_report_fn=capturing_create_failure_report,
            send_webhook_fn=noop_send_webhook,
        )

    assert len(captured) == 1
    assert captured[0]["failure_phase"] == "positions"


def test_positions_fail_calls_send_webhook():
    webhooks = []

    with pytest.raises(ValueError):
        extract_trips_for_all_Lines_and_vehicles(
            make_config(webhook_url="http://example.com/webhook"),
            get_recent_positions_fn=lambda c: pd.DataFrame(),
            validate_positions_fn=positions_fail_stub,
            create_failure_report_fn=noop_create_failure_report,
            send_webhook_fn=lambda summary, url: webhooks.append(summary),
        )

    assert len(webhooks) == 1


def test_positions_fail_final_report_not_called():
    final_report_calls = []

    def capturing_final_report(config, execution_id, run_ts, positions_result, trips_result, persistence_result, write_fn=None):
        final_report_calls.append(True)
        return {"summary": {"status": "PASS"}, "details": {}}

    with pytest.raises(ValueError):
        extract_trips_for_all_Lines_and_vehicles(
            make_config(),
            get_recent_positions_fn=lambda c: pd.DataFrame(),
            validate_positions_fn=positions_fail_stub,
            create_failure_report_fn=noop_create_failure_report,
            create_final_report_fn=capturing_final_report,
            send_webhook_fn=noop_send_webhook,
        )

    assert final_report_calls == []


def test_trip_extraction_failure_calls_create_failure_report_with_positions_result():
    captured = []
    df = make_positions_df(
        [
            {
                "veiculo_ts": BASE_TS,
                "extracao_ts": BASE_TS,
                "linha_lt": "1234-10",
                "veiculo_id": 100,
                "linha_sentido": 1,
                "is_circular": False,
            }
        ]
    )

    def failing_extract_trips(config, positions_dataframe):
        raise RuntimeError("trip extraction exploded")

    def capturing_create_failure_report(
        config,
        execution_id,
        run_ts,
        failure_phase,
        failure_message,
        positions_result,
        trips_result=None,
        persistence_result=None,
        write_fn=None,
    ):
        captured.append(
            {
                "failure_phase": failure_phase,
                "failure_message": failure_message,
                "positions_result": positions_result,
                "trips_result": trips_result,
                "persistence_result": persistence_result,
            }
        )
        return {"summary": {"status": "FAIL"}, "details": {}}

    with pytest.raises(RuntimeError, match="trip extraction exploded"):
        extract_trips_for_all_Lines_and_vehicles(
            make_config(),
            get_recent_positions_fn=lambda c: df,
            validate_positions_fn=positions_pass_stub,
            extract_trips_fn=failing_extract_trips,
            create_failure_report_fn=capturing_create_failure_report,
            send_webhook_fn=noop_send_webhook,
        )

    assert len(captured) == 1
    assert captured[0]["failure_phase"] == "trip_extraction"
    assert captured[0]["failure_message"] == "trip extraction exploded"
    assert captured[0]["positions_result"]["status"] == "PASS"
    assert captured[0]["trips_result"] is None
    assert captured[0]["persistence_result"] is None


def test_persistence_failure_calls_create_failure_report_with_partial_results():
    captured = []
    df = make_positions_df(
        [
            {
                "veiculo_ts": BASE_TS,
                "extracao_ts": BASE_TS,
                "linha_lt": "1234-10",
                "veiculo_id": 100,
                "linha_sentido": 1,
                "is_circular": False,
            }
        ]
    )
    extracted_trips = [{"trip_id": "1234-10-0"}]

    def failing_save_trips(config, trips):
        raise RuntimeError("save failed")

    def capturing_create_failure_report(
        config,
        execution_id,
        run_ts,
        failure_phase,
        failure_message,
        positions_result,
        trips_result=None,
        persistence_result=None,
        write_fn=None,
    ):
        captured.append(
            {
                "failure_phase": failure_phase,
                "failure_message": failure_message,
                "positions_result": positions_result,
                "trips_result": trips_result,
                "persistence_result": persistence_result,
            }
        )
        return {"summary": {"status": "FAIL"}, "details": {}}

    with pytest.raises(RuntimeError, match="save failed"):
        extract_trips_for_all_Lines_and_vehicles(
            make_config(),
            get_recent_positions_fn=lambda c: df,
            validate_positions_fn=positions_pass_stub,
            extract_trips_fn=lambda config, positions_dataframe: extracted_trips,
            validate_trips_fn=trips_pass_stub,
            save_trips_fn=failing_save_trips,
            create_failure_report_fn=capturing_create_failure_report,
            send_webhook_fn=noop_send_webhook,
        )

    assert len(captured) == 1
    assert captured[0]["failure_phase"] == "persistence"
    assert captured[0]["failure_message"] == "save failed"
    assert captured[0]["positions_result"]["status"] == "PASS"
    assert captured[0]["trips_result"]["status"] == "PASS"
    assert captured[0]["persistence_result"] is None


# ---------------------------------------------------------------------------
# Positions WARN → early report + webhook, pipeline continues to Phase 3
# ---------------------------------------------------------------------------


def test_positions_warn_calls_create_report_and_early_webhook():
    reports = []
    webhooks = []

    extract_trips_for_all_Lines_and_vehicles(
        make_config(webhook_url="http://example.com/webhook"),
        get_recent_positions_fn=lambda c: pd.DataFrame(
            [{"veiculo_ts": 1, "linha_lt": "x", "veiculo_id": 1, "linha_sentido": 1, "is_circular": False}]
        ),
        validate_positions_fn=positions_warn_stub,
        validate_trips_fn=trips_pass_stub,
        validate_persistence_fn=persistence_pass_stub,
        create_report_fn=lambda config, exec_id, run_ts, positions_result, write_fn=None: reports.append(positions_result) or {"summary": {"status": "WARN"}, "details": {}},
        create_final_report_fn=noop_create_final_report,
        send_webhook_fn=lambda summary, url: webhooks.append(summary),
        save_trips_fn=noop_save_trips,
        extract_trips_fn=lambda config, df: [],
    )

    assert len(reports) == 1
    assert len(webhooks) == 2  # early WARN webhook + final report webhook


def test_positions_warn_pipeline_continues_and_save_called():
    save = SaveCapture()

    extract_trips_for_all_Lines_and_vehicles(
        make_config(),
        get_recent_positions_fn=lambda c: pd.DataFrame(
            [{"veiculo_ts": 1, "linha_lt": "x", "veiculo_id": 1, "linha_sentido": 1, "is_circular": False}]
        ),
        validate_positions_fn=positions_warn_stub,
        validate_trips_fn=trips_pass_stub,
        validate_persistence_fn=persistence_pass_stub,
        create_report_fn=lambda config, exec_id, run_ts, positions_result, write_fn=None: {"summary": {"status": "WARN"}, "details": {}},
        create_final_report_fn=noop_create_final_report,
        send_webhook_fn=noop_send_webhook,
        save_trips_fn=save,
        extract_trips_fn=lambda config, df: [],
    )

    assert len(save.calls) == 1


# ---------------------------------------------------------------------------
# All phases PASS → final report saved, webhook sent once
# ---------------------------------------------------------------------------


def test_all_phases_pass_final_webhook_sent_once():
    webhooks = []
    df = make_positions_df(
        [
            {
                "veiculo_ts": BASE_TS,
                "extracao_ts": BASE_TS,
                "linha_lt": "1234-10",
                "veiculo_id": 100,
                "linha_sentido": 1,
                "is_circular": False,
            }
        ]
    )
    extract_trips_for_all_Lines_and_vehicles(
        make_config(webhook_url="http://example.com/webhook"),
        get_recent_positions_fn=lambda c: df,
        validate_positions_fn=positions_pass_stub,
        validate_trips_fn=trips_pass_stub,
        validate_persistence_fn=persistence_pass_stub,
        create_final_report_fn=noop_create_final_report,
        send_webhook_fn=lambda summary, url: webhooks.append(summary),
        save_trips_fn=noop_save_trips,
        extract_trips_fn=lambda config, df: [],
    )

    assert len(webhooks) == 1
    assert webhooks[0]["status"] == "PASS"


def test_all_phases_pass_final_report_status_pass():
    final_reports = []
    df = make_positions_df(
        [
            {
                "veiculo_ts": BASE_TS,
                "extracao_ts": BASE_TS,
                "linha_lt": "1234-10",
                "veiculo_id": 100,
                "linha_sentido": 1,
                "is_circular": False,
            }
        ]
    )

    def capturing_final_report(config, execution_id, run_ts, positions_result, trips_result, persistence_result, write_fn=None):
        final_reports.append((positions_result, trips_result, persistence_result))
        return {"summary": {"status": "PASS"}, "details": {}}

    extract_trips_for_all_Lines_and_vehicles(
        make_config(),
        get_recent_positions_fn=lambda c: df,
        validate_positions_fn=positions_pass_stub,
        validate_trips_fn=trips_pass_stub,
        validate_persistence_fn=persistence_pass_stub,
        create_final_report_fn=capturing_final_report,
        send_webhook_fn=noop_send_webhook,
        save_trips_fn=noop_save_trips,
        extract_trips_fn=lambda config, df: [],
    )

    assert len(final_reports) == 1
    positions_result, trips_result, persistence_result = final_reports[0]
    assert positions_result["status"] == "PASS"
    assert trips_result["status"] == "PASS"
    assert persistence_result["status"] == "PASS"


def test_trip_extraction_metrics_reach_final_report():
    final_reports = []
    df = make_positions_df(
        [
            {
                "veiculo_ts": BASE_TS,
                "extracao_ts": BASE_TS,
                "linha_lt": "1234-10",
                "veiculo_id": 100,
                "linha_sentido": 1,
                "is_circular": False,
            }
        ]
    )
    extraction_metrics = {
        "total_finished_trips": 1,
        "total_source_sentido_discrepancies": 3,
        "total_input_position_sanitization_drops": 7,
        "total_input_position_records": 42,
        "vehicle_line_groups_processed": 1,
    }

    def capturing_final_report(config, execution_id, run_ts, positions_result, trips_result, persistence_result, write_fn=None):
        final_reports.append(trips_result)
        return {"summary": {"status": "PASS"}, "details": {}}

    extract_trips_for_all_Lines_and_vehicles(
        make_config(),
        get_recent_positions_fn=lambda c: df,
        validate_positions_fn=positions_pass_stub,
        extract_trips_fn=lambda config, positions_dataframe: ([], extraction_metrics),
        validate_trips_fn=trips_pass_stub,
        validate_persistence_fn=persistence_pass_stub,
        create_final_report_fn=capturing_final_report,
        send_webhook_fn=noop_send_webhook,
        save_trips_fn=noop_save_trips,
    )

    assert len(final_reports) == 1
    assert final_reports[0]["source_sentido_discrepancies"] == 3
    assert final_reports[0]["sanitization_dropped_points"] == 7
    assert final_reports[0]["input_position_records"] == 42
    assert final_reports[0]["vehicle_line_groups_processed"] == 1


# ---------------------------------------------------------------------------
# Positions PASS → extraction and save proceed (existing behaviour)
# ---------------------------------------------------------------------------


def test_no_trips_extracted_save_called_with_empty_list():
    df = make_positions_df(
        [
            {
                "veiculo_ts": BASE_TS,
                "extracao_ts": BASE_TS,
                "linha_lt": "1234-10",
                "veiculo_id": 100,
                "linha_sentido": 1,
                "is_circular": False,
            },
            {
                "veiculo_ts": BASE_TS + timedelta(seconds=60),
                "extracao_ts": BASE_TS + timedelta(seconds=60),
                "linha_lt": "1234-10",
                "veiculo_id": 100,
                "linha_sentido": 1,
                "is_circular": False,
            },
        ]
    )
    save = SaveCapture()
    extract_trips_for_all_Lines_and_vehicles(
        make_config(),
        get_recent_positions_fn=lambda c: df,
        save_trips_fn=save,
        extract_trips_fn=lambda config, df: [],
        validate_positions_fn=positions_pass_stub,
        validate_persistence_fn=persistence_pass_stub,
        create_final_report_fn=noop_create_final_report,
        send_webhook_fn=noop_send_webhook,
    )
    assert len(save.calls) == 1
    assert save.calls[0] == []


def test_two_vehicles_save_called_once_with_combined_result():
    df = make_positions_df(
        [
            {
                "veiculo_ts": BASE_TS,
                "extracao_ts": BASE_TS,
                "linha_lt": "1234-10",
                "veiculo_id": 100,
                "linha_sentido": 1,
                "is_circular": False,
            },
            {
                "veiculo_ts": BASE_TS + timedelta(seconds=60),
                "extracao_ts": BASE_TS + timedelta(seconds=60),
                "linha_lt": "5678-20",
                "veiculo_id": 200,
                "linha_sentido": 1,
                "is_circular": False,
            },
        ]
    )
    save = SaveCapture()
    extract_trips_for_all_Lines_and_vehicles(
        make_config(),
        get_recent_positions_fn=lambda c: df,
        save_trips_fn=save,
        extract_trips_fn=lambda config, df: [],
        validate_positions_fn=positions_pass_stub,
        validate_persistence_fn=persistence_pass_stub,
        create_final_report_fn=noop_create_final_report,
        send_webhook_fn=noop_send_webhook,
    )
    assert len(save.calls) == 1
