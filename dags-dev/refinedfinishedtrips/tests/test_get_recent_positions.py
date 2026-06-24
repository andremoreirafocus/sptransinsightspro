import pandas as pd
import pytest
from datetime import datetime, timezone

from fakes.fake_duckdb_connection import FakeDuckDBConnection
from refinedfinishedtrips.services.get_recent_positions import get_recent_positions

LOGIC_DATE = datetime(2026, 4, 14, 10, 0, 0, tzinfo=timezone.utc)


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
    result = get_recent_positions(make_config(), LOGIC_DATE, duckdb_client=fake)
    assert result.equals(expected_df)


def test_sql_query_includes_distance_and_position_columns():
    expected_df = pd.DataFrame(
        [
            {
                "veiculo_ts": "2026-04-14",
                "linha_lt": "1234-10",
                "veiculo_id": 100,
                "veiculo_lat": -23.5,
                "veiculo_long": -46.6,
                "distance_to_first_stop": 50.0,
                "distance_to_last_stop": 3200.0,
            }
        ]
    )
    fake = FakeDuckDBConnection(df=expected_df)
    result = get_recent_positions(make_config(), LOGIC_DATE, duckdb_client=fake)
    assert "veiculo_lat" in result.columns
    assert "veiculo_long" in result.columns
    assert "distance_to_first_stop" in result.columns
    assert "distance_to_last_stop" in result.columns


def test_missing_config_key_raises_key_error():
    config = make_config()
    del config["general"]
    fake = FakeDuckDBConnection(df=pd.DataFrame())
    with pytest.raises(KeyError, match="general"):
        get_recent_positions(config, LOGIC_DATE, duckdb_client=fake)


def test_duckdb_error_raises_value_error():
    fake = FakeDuckDBConnection(raises=Exception("DuckDB connection failed"))
    with pytest.raises(ValueError, match="Data retrieval failed"):
        get_recent_positions(make_config(), LOGIC_DATE, duckdb_client=fake)


def test_connection_closed_after_successful_call():
    fake = FakeDuckDBConnection(df=pd.DataFrame())
    get_recent_positions(make_config(), LOGIC_DATE, duckdb_client=fake)
    assert fake.closed is True


def test_sql_selects_trip_linear_distance():
    fake = FakeDuckDBConnection(df=pd.DataFrame())
    get_recent_positions(make_config(), LOGIC_DATE, duckdb_client=fake)
    assert any("trip_linear_distance" in sql for sql in fake.executed_sql)


def test_sql_uses_logical_date_to_build_partition_and_hour_window():
    fake = FakeDuckDBConnection(df=pd.DataFrame())
    get_recent_positions(make_config(), LOGIC_DATE, duckdb_client=fake)
    executed_sql = fake.executed_sql[0]
    assert "year=2026/month=04/day=14" in executed_sql
    assert "hour::INTEGER >= 5" in executed_sql
    assert "hour::INTEGER <= 7" in executed_sql
