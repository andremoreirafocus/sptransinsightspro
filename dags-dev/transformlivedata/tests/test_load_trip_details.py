import pandas as pd
import pytest
from transformlivedata.services.load_trip_details import load_trip_details
from fakes.fake_duckdb_connection import FakeDuckDBConnection


def make_config():
    return {
        "general": {
            "storage": {
                "trusted_bucket": "trusted",
                "gtfs_folder": "gtfs",
            },
            "tables": {
                "trip_details_table_name": "trip_details",
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


def test_returns_dataframe():
    df = pd.DataFrame({"trip_id": ["t1", "t2"]})
    fake_con = FakeDuckDBConnection(df=df)
    result = load_trip_details(make_config(), duckdb_client=fake_con)
    assert list(result["trip_id"]) == ["t1", "t2"]


def test_connection_closed_after_success():
    df = pd.DataFrame({"trip_id": ["t1"]})
    fake_con = FakeDuckDBConnection(df=df)
    load_trip_details(make_config(), duckdb_client=fake_con)
    assert fake_con.closed


def test_duckdb_error_raises_runtime_error():
    fake_con = FakeDuckDBConnection(raises=RuntimeError("duckdb boom"))
    with pytest.raises(RuntimeError, match="Error fetching trip_details from storage"):
        load_trip_details(make_config(), duckdb_client=fake_con)


def test_missing_config_key_raises_value_error():
    config = make_config()
    del config["general"]["storage"]["trusted_bucket"]
    with pytest.raises(ValueError, match="Missing required configuration key"):
        load_trip_details(config)
