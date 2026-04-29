from typing import Any, Dict, Type

from pydantic import BaseModel, ConfigDict, ValidationError


class AnalysisConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    hours_window: int


class StorageConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    app_folder: str
    trusted_bucket: str
    metadata_bucket: str
    quality_report_folder: str


class TablesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    positions_table_name: str
    finished_trips_table_name: str


class QualityConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    freshness_warn_staleness_minutes: int
    freshness_fail_staleness_minutes: int
    gaps_warn_gap_minutes: int
    gaps_fail_gap_minutes: int
    gaps_recent_window_minutes: int
    trips_effective_window_threshold_minutes: int
    trips_min_trips_threshold: int


class NotificationsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    webhook_url: str


class GeneralConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    analysis: AnalysisConfig
    storage: StorageConfig
    tables: TablesConfig
    quality: QualityConfig
    notifications: NotificationsConfig


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
