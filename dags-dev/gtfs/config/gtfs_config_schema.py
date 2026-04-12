from typing import Any, Dict, Type

from pydantic import BaseModel, ConfigDict, ValidationError


class ExtractionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    local_downloads_folder: str


class StorageConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    app_folder: str
    gtfs_folder: str
    raw_bucket: str
    trusted_bucket: str


class TablesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    trip_details_table_name: str


class GeneralConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    extraction: ExtractionConfig
    storage: StorageConfig
    tables: TablesConfig


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
