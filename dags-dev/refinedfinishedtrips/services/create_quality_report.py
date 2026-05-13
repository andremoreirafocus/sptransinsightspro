import logging
from typing import Any, Callable, Dict, Literal, Optional

from infra.object_storage import write_generic_bytes_to_object_storage
from quality.reporting import (
    build_quality_report_path,
    build_quality_summary,
    save_quality_report,
)

logger = logging.getLogger(__name__)

PIPELINE_NAME = "refinedfinishedtrips"


def _count_failed_checks(result: Dict[str, Any]) -> int:
    return sum(1 for c in result.get("checks", []) if c.get("status") == "FAIL")


def _derive_overall_status(*results: Dict[str, Any]) -> Literal["PASS", "WARN", "FAIL"]:
    statuses = [r["status"] for r in results]
    if "FAIL" in statuses:
        return "FAIL"
    if "WARN" in statuses:
        return "WARN"
    return "PASS"


def _build_phase_details(
    positions_result: Dict[str, Any],
    trips_result: Optional[Dict[str, Any]] = None,
    persistence_result: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    phase_details = {"positions": positions_result}
    if trips_result is not None:
        phase_details["trip_extraction"] = trips_result
    if persistence_result is not None:
        phase_details["persistence"] = persistence_result
    return phase_details


def build_quality_report(
    execution_id: str,
    positions_result: Dict[str, Any],
    quality_report_path: str,
    status: Literal["PASS", "WARN", "FAIL"],
    failure_phase: Optional[str] = None,
    failure_message: Optional[str] = None,
    trips_result: Optional[Dict[str, Any]] = None,
    persistence_result: Optional[Dict[str, Any]] = None,
    column_lineage: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    items_failed = _count_failed_checks(positions_result)
    summary = build_quality_summary(
        pipeline=PIPELINE_NAME,
        execution_id=execution_id,
        status=status,
        items_failed=items_failed,
        quality_report_path=quality_report_path,
        failure_phase=failure_phase,
        failure_message=failure_message,
        positions_in_time_window_count=positions_result.get(
            "positions_in_time_window_count"
        ),
    )
    details = {
        "execution_id": execution_id,
        "status": status,
        "failure_phase": failure_phase,
        "failure_message": failure_message,
        "phases": _build_phase_details(positions_result, trips_result, persistence_result),
        "artifacts": {
            "quality_report_path": quality_report_path,
            "column_lineage": column_lineage or {},
        },
    }
    return {"summary": summary, "details": details}


def create_quality_report(
    config: Dict[str, Any],
    execution_id: str,
    run_ts: Any,
    positions_result: Dict[str, Any],
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
    report = build_quality_report(
        execution_id=execution_id,
        positions_result=positions_result,
        quality_report_path=report_path,
        status=positions_result["status"],
    )
    save_quality_report(
        report=report,
        path=report_path,
        connection_data=connection_data,
        write_fn=write_fn,
    )
    logger.info(f"Quality report ({positions_result['status']}) saved to {report_path}.")
    return report


def create_failure_quality_report(
    config: Dict[str, Any],
    execution_id: str,
    run_ts: Any,
    failure_phase: str,
    failure_message: str,
    positions_result: Dict[str, Any],
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
    report = build_quality_report(
        execution_id=execution_id,
        positions_result=positions_result,
        quality_report_path=report_path,
        status="FAIL",
        failure_phase=failure_phase,
        failure_message=failure_message,
        trips_result=trips_result,
        persistence_result=persistence_result,
        column_lineage=column_lineage,
    )
    save_quality_report(
        report=report,
        path=report_path,
        connection_data=connection_data,
        write_fn=write_fn,
    )
    logger.info(f"Failure quality report saved to {report_path}.")
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
    items_failed = sum(_count_failed_checks(r) for r in (positions_result, trips_result, persistence_result))
    summary = build_quality_summary(
        pipeline=PIPELINE_NAME,
        execution_id=execution_id,
        status=overall_status,
        items_failed=items_failed,
        quality_report_path=report_path,
        positions_in_time_window_count=positions_result.get("positions_in_time_window_count"),
        trips_extracted=trips_result.get("trips_extracted"),
        source_sentido_discrepancies=trips_result.get(
            "source_sentido_discrepancies"
        ),
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
    logger.info(f"Final quality report ({overall_status}) saved to {report_path}.")
    return report
