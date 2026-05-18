import pandas as pd
import pytest
from refinedsynctripdetails.services.save_trip_details_from_dataframe_to_refined import (
    save_trip_details_from_dataframe_to_refined,
)


def make_config(overrides=None):
    config = {
        "general": {
            "tables": {
                "trip_details_table_name": "refined.trip_details",
            },
        },
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
    if overrides:
        config.update(overrides)
    return config


def test_save_fn_called_with_correct_args():
    calls = []

    def fake_save(connection, df, table_name):
        calls.append((connection, df, table_name))

    df = pd.DataFrame({"trip_id": ["t1"]})
    save_trip_details_from_dataframe_to_refined(make_config(), df, save_fn=fake_save)

    assert len(calls) == 1
    _, called_df, called_table = calls[0]
    assert list(called_df["trip_id"]) == ["t1"]
    assert called_table == "refined.trip_details"


def test_save_fn_receives_correct_connection_keys():
    calls = []

    def fake_save(connection, df, table_name):
        calls.append(connection)

    df = pd.DataFrame({"trip_id": ["t1"]})
    save_trip_details_from_dataframe_to_refined(make_config(), df, save_fn=fake_save)

    conn = calls[0]
    assert conn["host"] == "localhost"
    assert conn["port"] == 5432
    assert conn["database"] == "testdb"
    assert conn["user"] == "user"
    assert conn["password"] == "pass"


def test_save_fn_error_propagates():
    def fail_save(connection, df, table_name):
        raise RuntimeError("db write failed")

    df = pd.DataFrame({"trip_id": ["t1"]})
    with pytest.raises(ValueError, match="Failed to update table"):
        save_trip_details_from_dataframe_to_refined(
            make_config(), df, save_fn=fail_save
        )
