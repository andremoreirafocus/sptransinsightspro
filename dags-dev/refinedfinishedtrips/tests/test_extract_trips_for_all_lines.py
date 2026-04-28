from datetime import datetime, timedelta, timezone

import pandas as pd

from refinedfinishedtrips.extract_trips_for_all_Lines_and_vehicles import (
    extract_trips_for_all_Lines_and_vehicles,
)

BASE_TS = datetime(2026, 4, 14, 10, 0, 0, tzinfo=timezone.utc)


def make_config():
    return {
        "general": {"tables": {"finished_trips_table_name": "finished_trips"}},
        "connections": {
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "testdb",
                "user": "user",
                "password": "pass",
            }
        },
    }


class SaveCapture:
    def __init__(self):
        self.calls = []

    def __call__(self, config, trips):
        self.calls.append(trips)


def make_positions_df(rows):
    return pd.DataFrame(rows)


def test_empty_positions_save_not_called():
    save = SaveCapture()
    extract_trips_for_all_Lines_and_vehicles(
        make_config(),
        get_recent_positions_fn=lambda c: pd.DataFrame(),
        save_trips_fn=save,
    )
    assert save.calls == []


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
    )
    assert len(save.calls) == 1
    assert save.calls[0] == []


def test_two_vehicles_save_called_once_with_combined_result():
    # Two distinct vehicles, each with a single position → no trips → save called once with []
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
    )
    assert len(save.calls) == 1
