import pandas as pd
import pytest

from fakes.fake_duckdb_connection import FakeDuckDBConnection
from refinedfinishedtrips.services.get_recent_positions import get_recent_positions


def make_config():
    return {
        "general": {
            "analysis": {"hours_window": "2"},
            "storage": {"trusted_bucket": "test-bucket", "app_folder": "data"},
            "tables": {"positions_table_name": "positions"},
        },
        "connections": {
            "object_storage": {
                "endpoint": "minio:9000",
                "access_key": "test_key",
                "secret_key": "test_secret",
            }
        },
    }


def test_returns_dataframe_from_duckdb_client():
    expected_df = pd.DataFrame(
        [{"veiculo_ts": "2026-04-14", "linha_lt": "1234-10", "veiculo_id": 100}]
    )
    fake = FakeDuckDBConnection(df=expected_df)
    result = get_recent_positions(make_config(), duckdb_client=fake)
    assert result.equals(expected_df)


def test_sql_query_includes_distance_columns():
    expected_df = pd.DataFrame(
        [
            {
                "veiculo_ts": "2026-04-14",
                "linha_lt": "1234-10",
                "veiculo_id": 100,
                "distance_to_first_stop": 50.0,
                "distance_to_last_stop": 3200.0,
            }
        ]
    )
    fake = FakeDuckDBConnection(df=expected_df)
    result = get_recent_positions(make_config(), duckdb_client=fake)
    assert "distance_to_first_stop" in result.columns
    assert "distance_to_last_stop" in result.columns


def test_missing_config_key_raises_value_error():
    config = make_config()
    del config["general"]
    fake = FakeDuckDBConnection(df=pd.DataFrame())
    with pytest.raises(ValueError):
        get_recent_positions(config, duckdb_client=fake)


def test_duckdb_error_raises_value_error():
    fake = FakeDuckDBConnection(raises=Exception("DuckDB connection failed"))
    with pytest.raises(ValueError, match="Data retrieval failed"):
        get_recent_positions(make_config(), duckdb_client=fake)


def test_connection_closed_after_successful_call():
    fake = FakeDuckDBConnection(df=pd.DataFrame())
    get_recent_positions(make_config(), duckdb_client=fake)
    assert fake.closed is True
