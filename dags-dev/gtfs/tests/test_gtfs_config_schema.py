import pytest

from gtfs.config.gtfs_config_schema import GeneralConfig, validate_general_input


def make_general():
    return {
        "extraction": {"local_downloads_folder": "gtfs_files"},
        "storage": {
            "app_folder": "sptrans",
            "gtfs_folder": "gtfs",
            "raw_bucket": "raw",
            "metadata_bucket": "metadata",
            "quality_report_folder": "quality-reports",
            "quarantined_subfolder": "quarantined",
            "staging_subfolder": "staging",
            "trusted_bucket": "trusted",
        },
        "tables": {"trip_details_table_name": "trip_details"},
        "notifications": {"webhook_url": "disabled"},
        "data_validations": {
            "expectations_validation": {
                "expectations_suites": [
                    "data_expectations_stop_times",
                    "data_expectations_stops",
                    "data_expectations_trip_details",
                ]
            }
        },
    }


def test_general_config_accepts_staging_and_data_validations():
    config = GeneralConfig.model_validate(make_general())
    assert config.storage.staging_subfolder == "staging"


def test_general_config_rejects_invalid_expectations_suites():
    data = make_general()
    data["data_validations"]["expectations_validation"]["expectations_suites"] = [
        "data_expectations_stops",
        "data_expectations_trip_details",
    ]
    with pytest.raises(ValueError):
        GeneralConfig.model_validate(data)


def test_general_config_rejects_missing_metadata_bucket():
    data = make_general()
    del data["storage"]["metadata_bucket"]
    with pytest.raises(ValueError):
        GeneralConfig.model_validate(data)


def test_general_config_rejects_missing_data_validations():
    data = make_general()
    del data["data_validations"]
    with pytest.raises(ValueError):
        GeneralConfig.model_validate(data)


def test_general_config_rejects_empty_storage_field():
    data = make_general()
    data["storage"]["quality_report_folder"] = " / "
    with pytest.raises(ValueError):
        GeneralConfig.model_validate(data)


def test_validate_general_input_returns_model_when_valid():
    model = validate_general_input(make_general(), GeneralConfig)
    assert model.storage.gtfs_folder == "gtfs"


def test_validate_general_input_raises_clear_path_on_invalid_data():
    data = make_general()
    data["storage"]["metadata_bucket"] = " "
    with pytest.raises(ValueError, match=r"Invalid config at 'storage\.metadata_bucket'"):
        validate_general_input(data, GeneralConfig)
