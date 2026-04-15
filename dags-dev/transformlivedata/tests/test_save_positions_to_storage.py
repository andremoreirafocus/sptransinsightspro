import pandas as pd
import pytest
from transformlivedata.services.save_positions_to_storage import (
    save_positions_to_storage,
)
from fakes.fake_duckdb_connection import FakeDuckDBConnection


def make_config():
    return {
        "general": {
            "storage": {
                "trusted_bucket": "trusted",
                "quarantined_bucket": "quarantined",
                "app_folder": "sptrans",
            },
            "tables": {
                "positions_table_name": "positions",
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


def make_df():
    return pd.DataFrame(
        {
            "extracao_ts": ["2026-02-15 10:35:00"],
            "veiculo_id": [1],
            "linha_lt": ["1000-10"],
        }
    )


def test_returns_early_when_df_is_none():
    fake_con = FakeDuckDBConnection()
    # Should not raise
    save_positions_to_storage(make_config(), None, "trusted", duckdb_client=fake_con)
    assert not fake_con.closed  # execute never called


def test_returns_early_when_df_is_empty():
    fake_con = FakeDuckDBConnection()
    save_positions_to_storage(
        make_config(), pd.DataFrame(), "trusted", duckdb_client=fake_con
    )
    assert not fake_con.closed


def test_invalid_target_bucket_raises_value_error():
    with pytest.raises(ValueError, match="Invalid target_bucket"):
        save_positions_to_storage(make_config(), make_df(), "invalid")


def test_trusted_bucket_resolved_from_config():
    fake_con = FakeDuckDBConnection()
    # Should not raise — trusted_bucket exists
    save_positions_to_storage(
        make_config(), make_df(), "trusted", duckdb_client=fake_con
    )


def test_quarantined_bucket_resolved_from_config():
    fake_con = FakeDuckDBConnection()
    save_positions_to_storage(
        make_config(), make_df(), "quarantined", duckdb_client=fake_con
    )


def test_missing_config_key_raises_value_error():
    config = make_config()
    del config["general"]["storage"]["trusted_bucket"]
    with pytest.raises(ValueError, match="Missing required configuration key"):
        save_positions_to_storage(config, make_df(), "trusted")


def test_duckdb_error_raises_value_error():
    fake_con = FakeDuckDBConnection(raises=RuntimeError("copy failed"))
    with pytest.raises(ValueError, match="Failed to save positions"):
        save_positions_to_storage(
            make_config(), make_df(), "trusted", duckdb_client=fake_con
        )
