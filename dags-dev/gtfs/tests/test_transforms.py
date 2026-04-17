import io

from gtfs.services.transforms import transform_and_validate_table


def make_config():
    return {
        "general": {
            "storage": {
                "raw_bucket": "raw",
                "trusted_bucket": "trusted",
                "gtfs_folder": "gtfs",
                "staging_subfolder": "staging",
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


def test_transform_and_validate_table_skips_validation_when_suite_missing():
    calls = []

    def fake_load(config, table_name):
        return io.BytesIO(b"stop_id,stop_name\n1,A")

    def fake_convert(df):
        return b"parquet"

    def fake_save(config, file_name, buffer, subfolder=None):
        calls.append((file_name, subfolder))

    result = transform_and_validate_table(
        make_config(),
        "routes",
        load_fn=fake_load,
        convert_fn=fake_convert,
        save_fn=fake_save,
    )

    assert result["table_name"] == "routes"
    assert result["is_valid"] is True
    assert result["errors"] == []
    assert result["expectations_summary"] is None
    assert result["staged_written"] is True
    assert calls[0] == ("routes.parquet", "staging")


def test_transform_and_validate_table_marks_invalid_when_gx_fails():
    config = make_config()
    config["data_expectations_stops"] = {
        "expectation_suite_name": "gtfs_stops",
        "expectations": [{"expectation_type": "expect_column_values_to_not_be_null"}],
    }

    def fake_load(config, table_name):
        return io.BytesIO(b"stop_id,stop_name\n1,A")

    def fake_convert(df):
        return b"parquet"

    def fake_save(config, file_name, buffer, subfolder=None):
        return None

    def fake_validate_expectations(df, suite):
        return {
            "expectations_summary": {
                "rows_failed": 1,
                "expectations_with_violations": 1,
                "expectations_failed_due_to_exceptions": 0,
            }
        }

    result = transform_and_validate_table(
        config,
        "stops",
        load_fn=fake_load,
        convert_fn=fake_convert,
        save_fn=fake_save,
        validate_expectations_fn=fake_validate_expectations,
    )

    assert result["is_valid"] is False
    assert result["staged_written"] is True
    assert any(err.startswith("gx_validation_failed:") for err in result["errors"])


def test_transform_and_validate_table_returns_error_when_load_fails():
    def fake_load(config, table_name):
        raise RuntimeError("read error")

    result = transform_and_validate_table(
        make_config(),
        "stops",
        load_fn=fake_load,
    )

    assert result["is_valid"] is False
    assert result["staged_written"] is False
    assert result["errors"] == ["load_failed:read error"]
