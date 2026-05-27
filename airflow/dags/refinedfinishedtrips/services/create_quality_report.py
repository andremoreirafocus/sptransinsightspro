from typing import Any, Callable, Dict, Literal, Optional

from infra.object_storage import write_generic_bytes_to_object_storage
from observability.structured_event_logger import get_structured_logger
from quality.reporting import (
    build_quality_report_path,
    build_quality_summary,
    save_quality_report,
)

structured_logger = get_structured_logger(logger_name=__name__)

PIPELINE_NAME = "refinedfinishedtrips"


def _derive_overall_status(*results: Dict[str, Any]) -> Literal["PASS", "WARN", "FAIL"]:
    statuses = [r["status"] for r in results]
    if "FAIL" in statuses:
        return "FAIL"
    if "WARN" in statuses:
        return "WARN"
    return "PASS"


def create_failure_quality_report(
    config: Dict[str, Any],
    execution_id: str,
    run_ts: Any,
    failure_phase: str,
    failure_message: str,
    positions_result: Optional[Dict[str, Any]] = None,
    trips_result: Optional[Dict[str, Any]] = None,
    persistence_result: Optional[Dict[str, Any]] = None,
    column_lineage: Optional[Dict[str, Any]] = None,
    write_fn: Callable[..., Any] = write_generic_bytes_to_object_storage,
) -> Dict[str, Any]:
    def get_config(config):
        storage = config["general"]["storage"]
        return (
            storage["metadata_bucket"],
            storage["quality_report_folder"],
            {**config["connections"]["object_storage"], "secure": False},
        )

    metadata_bucket, quality_report_folder, connection_data = get_config(config)
    report_path = build_quality_report_path(
        metadata_bucket=metadata_bucket,
        quality_report_folder=quality_report_folder,
        pipeline_name=PIPELINE_NAME,
        batch_ts=run_ts,
        filename_suffix=f"_{execution_id.replace('-', '')[:8]}",
    )

    phase_details: Dict[str, Any] = {}
    if positions_result is not None:
        phase_details["positions"] = positions_result
    if trips_result is not None:
        phase_details["trip_extraction"] = trips_result
    if persistence_result is not None:
        phase_details["persistence"] = persistence_result

    summary = build_quality_summary(
        pipeline=PIPELINE_NAME,
        execution_id=execution_id,
        status="FAIL",
        items_failed=0,
        quality_report_path=report_path,
        failure_phase=failure_phase,
        failure_message=failure_message,
        positions_in_time_window_count=positions_result.get("positions_in_time_window_count") if positions_result is not None else None,
    )
    details = {
        "execution_id": execution_id,
        "status": "FAIL",
        "failure_phase": failure_phase,
        "failure_message": failure_message,
        "phases": phase_details,
        "artifacts": {
            "quality_report_path": report_path,
            "column_lineage": column_lineage or {},
        },
    }
    report = {"summary": summary, "details": details}
    save_quality_report(
        report=report,
        path=report_path,
        connection_data=connection_data,
        write_fn=write_fn,
    )
    structured_logger.info(
        event="failure_quality_report_saved",
        message="Failure quality report saved",
        metadata={"path": report_path, "failure_phase": failure_phase},
    )
    return report


def create_final_quality_report(
    config: Dict[str, Any],
    execution_id: str,
    run_ts: Any,
    positions_result: Dict[str, Any],
    trips_result: Dict[str, Any],
    persistence_result: Dict[str, Any],
    column_lineage: Optional[Dict[str, Any]] = None,
    write_fn: Callable[..., Any] = write_generic_bytes_to_object_storage,
) -> Dict[str, Any]:
    def get_config(config):
        storage = config["general"]["storage"]
        return (
            storage["metadata_bucket"],
            storage["quality_report_folder"],
            {**config["connections"]["object_storage"], "secure": False},
        )

    metadata_bucket, quality_report_folder, connection_data = get_config(config)
    report_path = build_quality_report_path(
        metadata_bucket=metadata_bucket,
        quality_report_folder=quality_report_folder,
        pipeline_name=PIPELINE_NAME,
        batch_ts=run_ts,
        filename_suffix=f"_{execution_id.replace('-', '')[:8]}",
    )
    overall_status = _derive_overall_status(positions_result, trips_result, persistence_result)
    summary = build_quality_summary(
        pipeline=PIPELINE_NAME,
        execution_id=execution_id,
        status=overall_status,
        items_failed=0,
        quality_report_path=report_path,
        positions_in_time_window_count=positions_result.get("positions_in_time_window_count"),
        trips_extracted=trips_result.get("trips_extracted"),
        source_sentido_discrepancies=trips_result.get("source_sentido_discrepancies"),
        sanitization_dropped_points=trips_result.get("sanitization_dropped_points"),
        vehicle_line_groups_processed=trips_result.get("vehicle_line_groups_processed"),
        added_rows=persistence_result.get("added_rows"),
        previously_saved_rows=persistence_result.get("previously_saved_rows"),
    )
    details = {
        "execution_id": execution_id,
        "status": overall_status,
        "failure_phase": None,
        "failure_message": None,
        "phases": {
            "positions": positions_result,
            "trip_extraction": trips_result,
            "persistence": persistence_result,
        },
        "artifacts": {
            "quality_report_path": report_path,
            "column_lineage": column_lineage or {},
        },
    }
    report = {"summary": summary, "details": details}
    save_quality_report(
        report=report,
        path=report_path,
        connection_data=connection_data,
        write_fn=write_fn,
    )
    structured_logger.info(
        event="final_quality_report_saved",
        message="Final quality report saved",
        metadata={"path": report_path, "status": overall_status},
    )
    return report
