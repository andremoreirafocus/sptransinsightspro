from typing import Any, Dict, Type

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator


class ExtractionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    local_downloads_folder: str


class StorageConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    app_folder: str
    gtfs_folder: str
    raw_bucket: str
    metadata_bucket: str
    quality_report_folder: str
    quarantined_subfolder: str
    staging_subfolder: str
    trusted_bucket: str

    @field_validator(
        "metadata_bucket",
        "quality_report_folder",
        "quarantined_subfolder",
        "staging_subfolder",
    )
    @classmethod
    def validate_storage_fields(cls, value: str) -> str:
        normalized = value.strip().strip("/")
        if not normalized:
            raise ValueError("storage field must be non-empty")
        return normalized


class ExpectationsValidationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expectations_suites: list[str]

    @field_validator("expectations_suites")
    @classmethod
    def validate_expectations_suites(cls, value: list[str]) -> list[str]:
        expected_values = {
            "data_expectations_stops",
            "data_expectations_stop_times",
        }
        provided_values = [item.strip() for item in value]
        if len(provided_values) != 2 or set(provided_values) != expected_values:
            raise ValueError(
                "expectations_suites must contain only "
                "['data_expectations_stops', 'data_expectations_stop_times']"
            )
        return provided_values


class DataValidationsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expectations_validation: ExpectationsValidationConfig


class TablesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    trip_details_table_name: str


class NotificationsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    webhook_url: str


class GeneralConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    extraction: ExtractionConfig
    storage: StorageConfig
    tables: TablesConfig
    notifications: NotificationsConfig
    data_validations: DataValidationsConfig


def validate_general_input(
    general: Dict[str, Any],
    model: Type[BaseModel],
) -> BaseModel:
    try:
        return model.model_validate(general)
    except ValidationError as e:
        first_error = e.errors()[0]
        path = ".".join(str(p) for p in first_error.get("loc", []))
        msg = first_error.get("msg", "Invalid configuration")
        raise ValueError(f"Invalid config at '{path}': {msg}")
