from typing import Any, Callable, Dict, Literal, Optional

from infra.object_storage import write_generic_bytes_to_object_storage
from observability.structured_event_logger import get_structured_logger
from quality.reporting import (
    build_quality_report_path,
    build_quality_summary,
    save_quality_report,
)

structured_logger = get_structured_logger(logger_name=__name__)

PIPELINE_NAME = "refinedtripfacts"


def _derive_overall_status(quality_result: Dict[str, Any]) -> Literal["PASS", "WARN", "FAIL"]:
    return quality_result["status"]


def _get_storage_config(config: Dict[str, Any]):
    storage = config["general"]["storage"]
    return (
        storage["metadata_bucket"],
        storage["quality_report_folder"],
        {**config["connections"]["object_storage"], "secure": False},
    )


def create_final_quality_report(
    config: Dict[str, Any],
    execution_id: str,
    run_ts: Any,
    measurement_result: Dict[str, Any],
    dim_time_result: Dict[str, Any],
    creation_result: Dict[str, Any],
    persisted_metrics: Dict[str, Any],
    quality_result: Dict[str, Any],
    column_lineage: Optional[Dict[str, Any]] = None,
    write_fn: Callable[..., Any] = write_generic_bytes_to_object_storage,
) -> Dict[str, Any]:
    metadata_bucket, quality_report_folder, connection_data = _get_storage_config(config)
    report_path = build_quality_report_path(
        metadata_bucket=metadata_bucket,
        quality_report_folder=quality_report_folder,
        pipeline_name=PIPELINE_NAME,
        batch_ts=run_ts,
        filename_suffix=f"_{execution_id.replace('-', '')[:8]}",
    )
    overall_status = _derive_overall_status(quality_result)
    drift_detected = (column_lineage or {}).get("drift_detected", False)

    summary = build_quality_summary(
        pipeline=PIPELINE_NAME,
        execution_id=execution_id,
        status=overall_status,
        items_failed=0,
        quality_report_path=report_path,
        finished_trips_read=measurement_result.get("finished_trips_read"),
        facts_derived=creation_result.get("facts_derived"),
        inserted_rows=creation_result.get("inserted_rows"),
        skipped_rows=creation_result.get("skipped_rows"),
        persisted_facts=persisted_metrics.get("persisted_facts"),
        uncovered_dim_keys=persisted_metrics.get("uncovered_dim_keys"),
        loss_rate=quality_result["loss_rate"],
        drift_detected=drift_detected,
    )
    details = {
        "execution_id": execution_id,
        "status": overall_status,
        "failure_phase": None,
        "failure_message": None,
        "phases": {
            "input_trips_measurement": measurement_result,
            "dim_time_provisioning": dim_time_result,
            "trip_facts_creation": creation_result,
            "trip_facts_verification": persisted_metrics,
            "data_quality_validation": quality_result,
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


def create_failure_quality_report(
    config: Dict[str, Any],
    execution_id: str,
    run_ts: Any,
    failure_phase: str,
    failure_message: str,
    measurement_result: Optional[Dict[str, Any]] = None,
    dim_time_result: Optional[Dict[str, Any]] = None,
    creation_result: Optional[Dict[str, Any]] = None,
    persisted_metrics: Optional[Dict[str, Any]] = None,
    quality_result: Optional[Dict[str, Any]] = None,
    column_lineage: Optional[Dict[str, Any]] = None,
    write_fn: Callable[..., Any] = write_generic_bytes_to_object_storage,
) -> Dict[str, Any]:
    metadata_bucket, quality_report_folder, connection_data = _get_storage_config(config)
    report_path = build_quality_report_path(
        metadata_bucket=metadata_bucket,
        quality_report_folder=quality_report_folder,
        pipeline_name=PIPELINE_NAME,
        batch_ts=run_ts,
        filename_suffix=f"_{execution_id.replace('-', '')[:8]}",
    )

    extra: Dict[str, Any] = {}
    if measurement_result:
        extra["finished_trips_read"] = measurement_result.get("finished_trips_read")
    if persisted_metrics:
        extra.update({k: persisted_metrics[k] for k in ("persisted_facts", "uncovered_dim_keys") if k in persisted_metrics})
    if creation_result:
        extra.update({k: creation_result[k] for k in ("facts_derived", "inserted_rows", "skipped_rows") if k in creation_result})
    if quality_result:
        extra["loss_rate"] = quality_result["loss_rate"]
    summary = build_quality_summary(
        pipeline=PIPELINE_NAME,
        execution_id=execution_id,
        status="FAIL",
        items_failed=0,
        quality_report_path=report_path,
        failure_phase=failure_phase,
        failure_message=failure_message,
        **extra,
    )
    phase_details: Dict[str, Any] = {}
    if measurement_result is not None:
        phase_details["input_trips_measurement"] = measurement_result
    if dim_time_result is not None:
        phase_details["dim_time_provisioning"] = dim_time_result
    if creation_result is not None:
        phase_details["trip_facts_creation"] = creation_result
    if persisted_metrics is not None:
        phase_details["trip_facts_verification"] = persisted_metrics
    if quality_result is not None:
        phase_details["data_quality_validation"] = quality_result
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
