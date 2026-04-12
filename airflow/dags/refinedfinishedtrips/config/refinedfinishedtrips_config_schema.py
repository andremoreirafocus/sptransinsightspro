from typing import Any, Dict, Type

from pydantic import BaseModel, ConfigDict, ValidationError


class AnalysisConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    hours_window: int


class StorageConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    app_folder: str
    trusted_bucket: str


class TablesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    positions_table_name: str
    finished_trips_table_name: str


class GeneralConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    analysis: AnalysisConfig
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
