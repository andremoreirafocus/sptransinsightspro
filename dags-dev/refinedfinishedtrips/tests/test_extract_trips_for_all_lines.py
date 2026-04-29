from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from refinedfinishedtrips.extract_trips_for_all_Lines_and_vehicles import (
    extract_trips_for_all_Lines_and_vehicles,
)

BASE_TS = datetime(2026, 4, 14, 10, 0, 0, tzinfo=timezone.utc)


def make_config():
    return {
        "general": {
            "tables": {"finished_trips_table_name": "finished_trips"},
            "notifications": {"webhook_url": "disabled"},
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


def positions_pass_stub(df, config):
    return {
        "status": "PASS",
        "positions_in_time_window_count": len(df),
        "checks": [],
    }


def positions_warn_stub(df, config):
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


def positions_fail_stub(df, config):
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


def noop_create_failure_report(config, execution_id, run_ts, failure_phase, failure_message, positions_result, write_fn=None):
    return {"summary": {"status": "FAIL"}, "details": {}}


def noop_send_webhook(summary, webhook_url):
    pass


class SaveCapture:
    def __init__(self):
        self.calls = []

    def __call__(self, config, trips):
        self.calls.append(trips)


def make_positions_df(rows):
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Positions FAIL → pipeline stops, save not called
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

    def capturing_create_failure_report(config, execution_id, run_ts, failure_phase, failure_message, positions_result, write_fn=None):
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
            make_config(),
            get_recent_positions_fn=lambda c: pd.DataFrame(),
            validate_positions_fn=positions_fail_stub,
            create_failure_report_fn=noop_create_failure_report,
            send_webhook_fn=lambda summary, url: webhooks.append(summary),
        )

    assert len(webhooks) == 1


# ---------------------------------------------------------------------------
# Positions WARN → report + webhook sent, pipeline continues
# ---------------------------------------------------------------------------


def test_positions_warn_calls_create_report_and_webhook():
    reports = []
    webhooks = []

    extract_trips_for_all_Lines_and_vehicles(
        make_config(),
        get_recent_positions_fn=lambda c: pd.DataFrame(
            [{"veiculo_ts": 1, "linha_lt": "x", "veiculo_id": 1, "linha_sentido": 1, "is_circular": False}]
        ),
        validate_positions_fn=positions_warn_stub,
        create_report_fn=lambda config, exec_id, run_ts, positions_result, write_fn=None: reports.append(positions_result) or {"summary": {"status": "WARN"}, "details": {}},
        send_webhook_fn=lambda summary, url: webhooks.append(summary),
        save_trips_fn=lambda config, trips: None,
        extract_trips_fn=lambda df: [],
    )

    assert len(reports) == 1
    assert len(webhooks) == 1


def test_positions_warn_pipeline_continues_and_save_called():
    save = SaveCapture()

    extract_trips_for_all_Lines_and_vehicles(
        make_config(),
        get_recent_positions_fn=lambda c: pd.DataFrame(
            [{"veiculo_ts": 1, "linha_lt": "x", "veiculo_id": 1, "linha_sentido": 1, "is_circular": False}]
        ),
        validate_positions_fn=positions_warn_stub,
        create_report_fn=lambda config, exec_id, run_ts, positions_result, write_fn=None: {"summary": {"status": "WARN"}, "details": {}},
        send_webhook_fn=noop_send_webhook,
        save_trips_fn=save,
        extract_trips_fn=lambda df: [],
    )

    assert len(save.calls) == 1


# ---------------------------------------------------------------------------
# Positions PASS → extraction and save proceed
# ---------------------------------------------------------------------------


def test_no_trips_extracted_save_called_with_empty_list():
    # Single vehicle, no direction changes → no trips survive extraction
    df = make_positions_df(
        [
            {
                "veiculo_ts": BASE_TS,
                "linha_lt": "1234-10",
                "veiculo_id": 100,
                "linha_sentido": 1,
                "is_circular": False,
            },
            {
                "veiculo_ts": BASE_TS + timedelta(seconds=60),
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
        validate_positions_fn=positions_pass_stub,
    )
    assert len(save.calls) == 1
    assert save.calls[0] == []


def test_two_vehicles_save_called_once_with_combined_result():
    df = make_positions_df(
        [
            {
                "veiculo_ts": BASE_TS,
                "linha_lt": "1234-10",
                "veiculo_id": 100,
                "linha_sentido": 1,
                "is_circular": False,
            },
            {
                "veiculo_ts": BASE_TS + timedelta(seconds=60),
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
        validate_positions_fn=positions_pass_stub,
    )
    assert len(save.calls) == 1
