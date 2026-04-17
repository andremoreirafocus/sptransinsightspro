import pytest

from gtfs.config.gtfs_config_schema import GeneralConfig


def make_general():
    return {
        "extraction": {"local_downloads_folder": "gtfs_files"},
        "storage": {
            "app_folder": "sptrans",
            "gtfs_folder": "gtfs",
            "raw_bucket": "raw",
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
