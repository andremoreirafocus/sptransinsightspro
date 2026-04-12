from typing import Any, Dict, Type

from pydantic import BaseModel, ConfigDict, ValidationError


class StorageConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    trusted_bucket: str
    gtfs_folder: str


class TablesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    trip_details_table_name: str


class GeneralConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
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
