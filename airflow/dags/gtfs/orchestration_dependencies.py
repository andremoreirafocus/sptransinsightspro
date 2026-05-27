from dataclasses import dataclass
from typing import Callable

from gtfs.services.create_data_quality_report import (
    create_data_quality_report,
    create_failure_quality_report,
)
from gtfs.services.create_save_trip_details import (
    create_trip_details_table_and_fill_missing_data,
)
from gtfs.services.extract_gtfs_files import extract_gtfs_files
from gtfs.services.relocate_staged_trusted_files import relocate_staged_trusted_files
from gtfs.services.save_files_to_raw_storage import save_files_to_raw_storage
from gtfs.services.transforms import transform_and_validate_table
from gtfs.services.validate_raw_gtfs_files import validate_raw_gtfs_files
from infra.object_storage import (
    read_file_from_object_storage_to_bytesio,
    write_generic_bytes_to_object_storage,
)
from pipeline_configurator.config import get_config
from quality.validate_expectations import validate_expectations


@dataclass(frozen=True)
class GtfsOrchestrationDependencies:
    get_config: Callable[..., dict]
    extract_gtfs_files: Callable
    validate_raw_gtfs_files: Callable
    save_files_to_raw_storage: Callable
    transform_and_validate_table: Callable
    relocate_staged_trusted_files: Callable
    create_trip_details_table_and_fill_missing_data: Callable
    read_file_from_object_storage_to_bytesio: Callable
    validate_expectations: Callable
    create_data_quality_report: Callable
    create_failure_quality_report: Callable
    write_fn: Callable


def get_gtfs_orchestration_dependencies() -> GtfsOrchestrationDependencies:
    return GtfsOrchestrationDependencies(
        get_config=get_config,
        extract_gtfs_files=extract_gtfs_files,
        validate_raw_gtfs_files=validate_raw_gtfs_files,
        save_files_to_raw_storage=save_files_to_raw_storage,
        transform_and_validate_table=transform_and_validate_table,
        relocate_staged_trusted_files=relocate_staged_trusted_files,
        create_trip_details_table_and_fill_missing_data=create_trip_details_table_and_fill_missing_data,
        read_file_from_object_storage_to_bytesio=read_file_from_object_storage_to_bytesio,
        validate_expectations=validate_expectations,
        create_data_quality_report=create_data_quality_report,
        create_failure_quality_report=create_failure_quality_report,
        write_fn=write_generic_bytes_to_object_storage,
    )
