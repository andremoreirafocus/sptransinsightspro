import pandas as pd
import pytest
from updatelatestpositions.services.create_latest_positions import (
    create_latest_positions_table,
)
from fakes.fake_duckdb_connection import FakeDuckDBConnection


def make_config():
    return {
        "general": {
            "storage": {
                "trusted_bucket": "trusted",
                "app_folder": "sptrans",
            },
            "tables": {
                "positions_table_name": "positions",
                "latest_positions_table_name": "refined.latest_positions",
            },
        },
        "connections": {
            "object_storage": {
                "endpoint": "localhost",
                "access_key": "key",
                "secret_key": "secret",
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "testdb",
                "user": "user",
                "password": "pass",
            },
        },
    }


def test_returns_early_when_no_path_found():
    save_calls = []

    def fake_get_path(config):
        return None

    def fake_save(connection, df, table_name):
        save_calls.append(True)

    create_latest_positions_table(
        make_config(),
        get_path_fn=fake_get_path,
        save_fn=fake_save,
    )
    assert save_calls == []


def test_save_called_with_correct_table_name():
    save_calls = []

    def fake_get_path(config):
        return "s3://trusted/sptrans/positions/year=2026/month=01/day=01/hour=10/data.parquet"

    def fake_save(connection, df, table_name):
        save_calls.append(table_name)

    df = pd.DataFrame(
        {
            "veiculo_ts": ["2026-01-01 10:00:00"],
            "veiculo_id": ["v1"],
            "veiculo_lat": [-23.5],
            "veiculo_long": [-46.6],
            "linha_lt": ["1000"],
            "linha_sentido": [1],
        }
    )
    fake_con = FakeDuckDBConnection(df=df)

    create_latest_positions_table(
        make_config(),
        get_path_fn=fake_get_path,
        duckdb_client=fake_con,
        save_fn=fake_save,
    )
    assert save_calls == ["refined.latest_positions"]


def test_duckdb_error_raises_value_error():
    def fake_get_path(config):
        return "s3://trusted/data.parquet"

    fake_con = FakeDuckDBConnection(raises=RuntimeError("duckdb boom"))

    with pytest.raises(ValueError, match="Update failed"):
        create_latest_positions_table(
            make_config(),
            get_path_fn=fake_get_path,
            duckdb_client=fake_con,
            save_fn=lambda *a: None,
        )


def test_missing_config_key_raises_value_error():
    config = make_config()
    del config["connections"]["database"]["host"]
    with pytest.raises(ValueError, match="Missing required configuration key"):
        create_latest_positions_table(config)
