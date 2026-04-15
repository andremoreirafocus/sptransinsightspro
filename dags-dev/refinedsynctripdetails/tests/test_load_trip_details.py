import pandas as pd
import pytest
from refinedsynctripdetails.services.load_trip_details_from_storage_to_dataframe import (
    load_trip_details_from_storage_to_dataframe,
)
from fakes.fake_duckdb_connection import FakeDuckDBConnection


def make_config(overrides=None):
    config = {
        "general": {
            "storage": {
                "trusted_bucket": "trusted",
                "gtfs_folder": "gtfs",
            },
            "tables": {
                "trip_details_table_name": "refined.trip_details",
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
    if overrides:
        config.update(overrides)
    return config


def test_returns_dataframe_from_duckdb_client():
    df = pd.DataFrame({"trip_id": ["t1", "t2"]})
    fake_con = FakeDuckDBConnection(df=df)
    result = load_trip_details_from_storage_to_dataframe(
        make_config(), duckdb_client=fake_con
    )
    assert isinstance(result, pd.DataFrame)
    assert list(result["trip_id"]) == ["t1", "t2"]


def test_connection_closed_after_successful_call():
    df = pd.DataFrame({"trip_id": ["t1"]})
    fake_con = FakeDuckDBConnection(df=df)
    load_trip_details_from_storage_to_dataframe(make_config(), duckdb_client=fake_con)
    assert fake_con.closed


def test_duckdb_error_raises_value_error():
    fake_con = FakeDuckDBConnection(raises=RuntimeError("duckdb boom"))
    with pytest.raises(ValueError, match="Error fetching trip_details from storage"):
        load_trip_details_from_storage_to_dataframe(
            make_config(), duckdb_client=fake_con
        )


def test_missing_storage_key_raises_value_error():
    config = make_config()
    del config["general"]["storage"]["trusted_bucket"]
    with pytest.raises(ValueError, match="Missing required configuration key"):
        load_trip_details_from_storage_to_dataframe(config)


def test_missing_tables_key_raises_value_error():
    config = make_config()
    del config["general"]["tables"]["trip_details_table_name"]
    with pytest.raises(ValueError, match="Missing required configuration key"):
        load_trip_details_from_storage_to_dataframe(config)


def test_table_name_without_schema_prefix():
    """trip_details_table_name without a dot should be used as-is in the s3 path."""
    config = make_config()
    config["general"]["tables"]["trip_details_table_name"] = "trip_details"
    df = pd.DataFrame({"trip_id": ["t1"]})
    fake_con = FakeDuckDBConnection(df=df)
    result = load_trip_details_from_storage_to_dataframe(config, duckdb_client=fake_con)
    assert list(result["trip_id"]) == ["t1"]
