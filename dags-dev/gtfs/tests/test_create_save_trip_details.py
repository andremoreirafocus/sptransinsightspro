import pytest
from fakes.fake_duckdb_connection import FakeDuckDBConnection
from gtfs.services.create_save_trip_details import (
    create_trip_details_table_and_fill_missing_data,
)


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


def test_missing_config_key_raises_value_error():
    config = make_config()
    del config["general"]["storage"]["trusted_bucket"]
    with pytest.raises(ValueError, match="Missing required configuration key"):
        create_trip_details_table_and_fill_missing_data(config)


def test_duckdb_error_raises_value_error():
    fake_con = FakeDuckDBConnection(raises=RuntimeError("duckdb boom"))
    with pytest.raises(ValueError, match="An unexpected error occurred"):
        create_trip_details_table_and_fill_missing_data(
            make_config(), duckdb_client=fake_con
        )


def test_connection_closed_after_error():
    fake_con = FakeDuckDBConnection(raises=RuntimeError("boom"))
    try:
        create_trip_details_table_and_fill_missing_data(
            make_config(), duckdb_client=fake_con
        )
    except ValueError:
        pass
    assert fake_con.closed
