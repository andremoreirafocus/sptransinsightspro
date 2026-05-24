import io

import pandas as pd
import pytest
from gtfs.gtfs import create_trip_details, StageExecutionError
from gtfs.tests.fakes.fake_gtfs_orchestration_dependencies import (
    FakeGtfsOrchestrationDependencies,
)


def make_run_context():
    return {"execution_id": "exec-test", "batch_ts": "2026-04-19T10:00:00+00:00"}


def build_trip_details_parquet_buffer():
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
            "trip_linear_distance": [1000],
            "is_circular": [False],
        }
    )
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer


def build_trip_details_parquet_buffer_with_drift():
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
            "trip_linear_distance": [1000],
            "is_circular": [False],
            "new_field": ["x"],
        }
    )
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer


def test_create_trip_details_moves_to_final_on_success():
    deps, recorder = FakeGtfsOrchestrationDependencies.create_scenario(
        storage_buffer=build_trip_details_parquet_buffer(),
        expectations_result={
            "valid_df": pd.DataFrame(),
            "invalid_df": None,
            "expectations_summary": {
                "rows_failed": 0,
                "expectations_with_violations": 0,
                "expectations_failed_due_to_exceptions": 0,
            },
        },
        relocation_result={"status": "SUCCESS", "moved": [], "errors": []},
    )

    result = create_trip_details(make_run_context(), {}, deps)

    stage = result["enrichment"]
    assert stage["status"] == "PASS"
    assert stage["relocation_status"] == "SUCCESS"
    assert stage["artifacts"]["column_lineage"]["warning"] is None
    assert "relocation_details" in stage
    assert stage["relocation_details"]["errors"] == []
    assert recorder.relocate_calls == [
        (
            "final",
            [
                {
                    "table_name": "trip_details",
                    "staging_object_name": "gtfs/staging/trip_details.parquet",
                    "staged_written": True,
                }
            ],
        )
    ]


def test_create_trip_details_sets_lineage_warning_on_drift():
    deps, recorder = FakeGtfsOrchestrationDependencies.create_scenario(
        storage_buffer=build_trip_details_parquet_buffer_with_drift(),
        expectations_result={
            "valid_df": pd.DataFrame(),
            "invalid_df": None,
            "expectations_summary": {
                "rows_failed": 0,
                "expectations_with_violations": 0,
                "expectations_failed_due_to_exceptions": 0,
            },
        },
        relocation_result={"status": "SUCCESS", "moved": [], "errors": []},
    )

    result = create_trip_details(make_run_context(), {}, deps)

    stage = result["enrichment"]
    assert stage["status"] == "PASS"
    assert stage["artifacts"]["column_lineage"]["warning"] == "lineage drift detected"
    assert "relocation_details" in stage
    assert stage["relocation_details"]["errors"] == []
    assert recorder.relocate_calls == [
        (
            "final",
            [
                {
                    "table_name": "trip_details",
                    "staging_object_name": "gtfs/staging/trip_details.parquet",
                    "staged_written": True,
                }
            ],
        )
    ]


def test_create_trip_details_quarantines_on_gx_failure():
    deps, recorder = FakeGtfsOrchestrationDependencies.create_scenario(
        storage_buffer=build_trip_details_parquet_buffer(),
        expectations_result={
            "valid_df": pd.DataFrame(),
            "invalid_df": pd.DataFrame({"col": [1]}),
            "expectations_summary": {
                "rows_failed": 1,
                "expectations_with_violations": 1,
                "expectations_failed_due_to_exceptions": 0,
            },
        },
        relocation_result={"status": "SUCCESS", "moved": [], "errors": []},
    )

    with pytest.raises(StageExecutionError) as excinfo:
        create_trip_details(make_run_context(), {}, deps)

    stage_result = excinfo.value.stage_result
    assert "relocation_details" in stage_result
    assert stage_result["relocation_details"]["errors"] == []
    assert recorder.relocate_calls == [
        (
            "quarantine",
            [
                {
                    "table_name": "trip_details",
                    "staging_object_name": "gtfs/staging/trip_details.parquet",
                    "staged_written": True,
                }
            ],
        )
    ]


def test_create_trip_details_quarantines_when_validation_raises_after_staging():
    deps, recorder = FakeGtfsOrchestrationDependencies.create_scenario(
        storage_read_raises=RuntimeError("read failed"),
        relocation_result={"status": "SUCCESS", "moved": [], "errors": []},
    )

    with pytest.raises(StageExecutionError) as excinfo:
        create_trip_details(make_run_context(), {}, deps)

    stage_result = excinfo.value.stage_result
    assert "relocation_details" in stage_result
    assert stage_result["relocation_details"]["errors"] == []
    assert recorder.relocate_calls == [
        (
            "quarantine",
            [
                {
                    "table_name": "trip_details",
                    "staging_object_name": "gtfs/staging/trip_details.parquet",
                    "staged_written": True,
                }
            ],
        )
    ]
