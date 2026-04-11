from typing import Any, Dict, Type

from pydantic import BaseModel, ValidationError, ConfigDict


class StorageConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    raw_bucket: str
    trusted_bucket: str
    quarantined_bucket: str
    metadata_bucket: str
    app_folder: str
    gtfs_folder: str
    quality_report_folder: str


class TablesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    positions_table_name: str
    trip_details_table_name: str
    raw_events_table_name: str


class CompressionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    raw_data_compression: bool
    raw_data_compression_extension: str


class GeneralConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    storage: StorageConfig
    tables: TablesConfig
    compression: CompressionConfig


class ObjectStorageConnection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    endpoint: str
    access_key: str
    secret_key: str


class DatabaseConnection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: str


class ConnectionsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    object_storage: ObjectStorageConnection
    database: DatabaseConnection


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
