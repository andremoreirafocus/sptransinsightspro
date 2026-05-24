import io
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import pandas as pd

from gtfs.orchestration_dependencies import (
    GtfsOrchestrationDependencies,
)

_TABLE_NAMES = ["stops", "stop_times", "routes", "trips", "frequencies", "calendar"]


def _default_pipeline_config() -> Dict[str, Any]:
    return {
        "general": {
            "notifications": {"webhook_url": "disabled"},
            "storage": {
                "trusted_bucket": "trusted",
                "gtfs_folder": "gtfs",
                "metadata_bucket": "meta",
                "quality_report_folder": "quality",
                "staging_subfolder": "staging",
            },
            "tables": {
                "trip_details_table_name": "trip_details",
            },
        },
        "connections": {
            "object_storage": {
                "endpoint": "localhost",
                "access_key": "key",
                "secret_key": "secret",
            }
        },
        "data_expectations_trip_details": {
            "expectation_suite_name": "gtfs_trip_details",
            "expectations": [{"expectation_type": "expect_column_values_to_not_be_null"}],
        },
    }


def _default_results_by_table() -> Dict[str, Any]:
    return {
        table_name: {
            "table_name": table_name,
            "staging_object_name": f"gtfs/staging/{table_name}.parquet",
            "is_valid": True,
            "errors": [],
            "expectations_summary": None,
            "staged_written": True,
        }
        for table_name in _TABLE_NAMES
    }


def _default_storage_buffer() -> io.BytesIO:
    df = pd.DataFrame(
        {
            "trip_id": ["t1"],
            "first_stop_id": [1],
            "first_stop_name": ["A"],
            "first_stop_lat": [-23.5],
            "first_stop_lon": [-46.6],
            "last_stop_id": [2],
            "last_stop_name": ["B"],
            "last_stop_lat": [-23.4],
            "last_stop_lon": [-46.5],
            "trip_linear_distance": [1000.0],
            "is_circular": [False],
        }
    )
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer


def _default_expectations_result() -> Dict[str, Any]:
    return {
        "valid_df": pd.DataFrame(),
        "invalid_df": None,
        "expectations_summary": {
            "rows_failed": 0,
            "expectations_with_violations": 0,
            "expectations_failed_due_to_exceptions": 0,
        },
    }


def _default_creation_result() -> Dict[str, Any]:
    return {
        "table_name": "trip_details",
        "staging_object_name": "gtfs/staging/trip_details.parquet",
        "row_count": 100,
        "staged_written": True,
    }


@dataclass
class GtfsOrchestrationCallRecorder:
    save_raw_calls: list = field(default_factory=list)
    relocate_calls: list = field(default_factory=list)
    create_report_calls: list = field(default_factory=list)
    create_failure_report_calls: list = field(default_factory=list)


class FakeGtfsOrchestrationDependencies:
    @classmethod
    def create_scenario(
        cls,
        *,
        config_raises: Optional[Exception] = None,
        extracted_files=None,
        extract_raises: Optional[Exception] = None,
        validation_result=None,
        validate_raw_raises: Optional[Exception] = None,
        save_raw_raises: Optional[Exception] = None,
        results_by_table=None,
        transform_raises: Optional[Exception] = None,
        relocation_result=None,
        relocate_raises: Optional[Exception] = None,
        creation_result=None,
        create_trip_details_raises: Optional[Exception] = None,
        storage_buffer=None,
        storage_read_raises: Optional[Exception] = None,
        expectations_result=None,
        expectations_raises: Optional[Exception] = None,
        quality_report_result=None,
        create_report_raises: Optional[Exception] = None,
        create_failure_report_raises: Optional[Exception] = None,
    ):
        recorder = GtfsOrchestrationCallRecorder()

        _extracted_files = extracted_files if extracted_files is not None else [
            f"{t}.txt" for t in _TABLE_NAMES
        ]
        _validation_result = validation_result if validation_result is not None else {
            "is_valid": True,
            "errors_by_file": {},
            "validated_files_count": len(_TABLE_NAMES),
        }
        _results_by_table = results_by_table if results_by_table is not None else _default_results_by_table()
        _relocation_result = relocation_result if relocation_result is not None else {
            "status": "SUCCESS",
            "moved": [],
            "errors": [],
        }
        _creation_result = creation_result if creation_result is not None else _default_creation_result()
        _storage_buffer = storage_buffer if storage_buffer is not None else _default_storage_buffer()
        _expectations_result = expectations_result if expectations_result is not None else _default_expectations_result()
        _quality_report_result = quality_report_result if quality_report_result is not None else {
            "summary": {"execution_id": "test", "status": "SUCCEEDED"}
        }

        def get_config(*args, **kwargs):
            if config_raises:
                raise config_raises
            return _default_pipeline_config()

        def fake_extract_gtfs_files(pipeline_config):
            if extract_raises:
                raise extract_raises
            return _extracted_files

        def fake_validate_raw_gtfs_files(pipeline_config, files_list):
            if validate_raw_raises:
                raise validate_raw_raises
            return _validation_result

        def fake_save_files_to_raw_storage(pipeline_config, files_list, failed=False):
            if save_raw_raises:
                raise save_raw_raises
            recorder.save_raw_calls.append({"failed": failed})

        def fake_transform_and_validate_table(pipeline_config, table_name):
            if transform_raises:
                raise transform_raises
            return _results_by_table[table_name]

        def fake_relocate_staged_trusted_files(pipeline_config, staged_results, target):
            if relocate_raises:
                raise relocate_raises
            recorder.relocate_calls.append((target, staged_results))
            return _relocation_result

        def fake_create_trip_details(pipeline_config):
            if create_trip_details_raises:
                raise create_trip_details_raises
            return _creation_result

        def fake_read_file_from_object_storage_to_bytesio(conn_data, bucket_name=None, object_name=None):
            if storage_read_raises:
                raise storage_read_raises
            return _storage_buffer

        def fake_validate_expectations(df, suite):
            if expectations_raises:
                raise expectations_raises
            return _expectations_result

        def fake_create_data_quality_report(**kwargs):
            if create_report_raises:
                raise create_report_raises
            recorder.create_report_calls.append(kwargs)
            return _quality_report_result

        def fake_create_failure_quality_report(**kwargs):
            if create_failure_report_raises:
                raise create_failure_report_raises
            recorder.create_failure_report_calls.append(kwargs)
            return {"summary": {"execution_id": "test", "status": "FAILED"}}

        def fake_write_fn(*args, **kwargs):
            pass

        deps = GtfsOrchestrationDependencies(
            get_config=get_config,
            extract_gtfs_files=fake_extract_gtfs_files,
            validate_raw_gtfs_files=fake_validate_raw_gtfs_files,
            save_files_to_raw_storage=fake_save_files_to_raw_storage,
            transform_and_validate_table=fake_transform_and_validate_table,
            relocate_staged_trusted_files=fake_relocate_staged_trusted_files,
            create_trip_details_table_and_fill_missing_data=fake_create_trip_details,
            read_file_from_object_storage_to_bytesio=fake_read_file_from_object_storage_to_bytesio,
            validate_expectations=fake_validate_expectations,
            create_data_quality_report=fake_create_data_quality_report,
            create_failure_quality_report=fake_create_failure_quality_report,
            write_fn=fake_write_fn,
        )
        return deps, recorder
