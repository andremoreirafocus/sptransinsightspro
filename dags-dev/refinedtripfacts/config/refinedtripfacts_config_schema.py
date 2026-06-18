from typing import Any, Dict, Type

from pydantic import BaseModel, ConfigDict, ValidationError


class TablesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    finished_trips_table_name: str
    trip_facts_table_name: str
    dim_time_table_name: str


class QualityConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    completeness_loss_rate_warn_threshold: float
    completeness_loss_rate_fail_threshold: float
    avg_speed_kmh_max: float


class GeneralConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tables: TablesConfig
    quality: QualityConfig


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
