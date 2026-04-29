from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from infra.object_storage import write_generic_bytes_to_object_storage
from quality.reporting import (
    build_quality_report_path,
    build_quality_summary,
    save_quality_report,
)


def build_data_quality_report(
    execution_id: str,
    status: str,
    stage_results: Dict[str, Dict[str, Any]],
    quality_report_path: str,
    failure_phase: Optional[str] = None,
    failure_message: Optional[str] = None,
) -> Dict[str, Any]:
    validated_items_count = sum(
        int(stage.get("validated_items_count", 0)) for stage in stage_results.values()
    )
    if failure_phase:
        failure_stage_data = stage_results.get(failure_phase, {})
        error_details = failure_stage_data.get("error_details", {})
        items_failed = sum(len(v) for v in error_details.values() if isinstance(v, dict))
    else:
        items_failed = 0
    failure_stage_data = stage_results.get(failure_phase, {}) if failure_phase else {}
    summary = build_quality_summary(
        pipeline="gtfs",
        execution_id=execution_id,
        status=status,
        items_failed=items_failed,
        quality_report_path=quality_report_path,
        failure_phase=failure_phase,
        failure_message=failure_message,
        stage="pipeline",
        validated_items_count=validated_items_count,
        relocation_status=failure_stage_data.get("relocation_status"),
        relocation_error=failure_stage_data.get("relocation_error"),
    )
    details = {
        "execution_id": execution_id,
        "status": status,
        "failure_phase": failure_phase,
        "failure_message": failure_message,
        "stages": stage_results,
        "artifacts": {
            "quality_report_path": quality_report_path,
        },
    }
    return {"summary": summary, "details": details}


def _build_report_path(config: Dict[str, Any], batch_ts: Any, execution_id: str) -> str:
    storage = config["general"]["storage"]
    return build_quality_report_path(
        metadata_bucket=storage["metadata_bucket"],
        quality_report_folder=storage["quality_report_folder"],
        pipeline_name="gtfs",
        batch_ts=batch_ts,
        filename_suffix=f"_{execution_id.replace('-', '')[:8]}",
    )


def create_data_quality_report(
    config: Dict[str, Any],
    execution_id: str,
    stage_results: Dict[str, Dict[str, Any]],
    batch_ts: Optional[Any] = None,
    write_fn: Callable[..., Any] = write_generic_bytes_to_object_storage,
) -> Dict[str, Any]:
    batch_ts_value = batch_ts or datetime.now(timezone.utc).isoformat()
    report_path = _build_report_path(config, batch_ts_value, execution_id)
    report = build_data_quality_report(
        execution_id=execution_id,
        status="PASS",
        stage_results=stage_results,
        quality_report_path=report_path,
    )
    connection_data = {**config["connections"]["object_storage"], "secure": False}
    save_quality_report(
        report=report,
        path=report_path,
        connection_data=connection_data,
        write_fn=write_fn,
    )
    return report


def create_failure_quality_report(
    config: Dict[str, Any],
    execution_id: str,
    failure_phase: str,
    failure_message: str,
    stage_results: Dict[str, Dict[str, Any]],
    batch_ts: Optional[Any] = None,
    write_fn: Callable[..., Any] = write_generic_bytes_to_object_storage,
) -> Dict[str, Any]:
    batch_ts_value = batch_ts or datetime.now(timezone.utc).isoformat()
    report_path = _build_report_path(config, batch_ts_value, execution_id)
    report = build_data_quality_report(
        execution_id=execution_id,
        status="FAIL",
        stage_results=stage_results,
        quality_report_path=report_path,
        failure_phase=failure_phase,
        failure_message=failure_message,
    )
    connection_data = {**config["connections"]["object_storage"], "secure": False}
    save_quality_report(
        report=report,
        path=report_path,
        connection_data=connection_data,
        write_fn=write_fn,
    )
    return report
