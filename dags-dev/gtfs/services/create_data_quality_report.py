from datetime import datetime, timezone
from typing import Any, Dict, Optional
import json

from infra.object_storage import write_generic_bytes_to_object_storage


def _count_failed_items(error_details: Dict[str, Any]) -> int:
    for value in error_details.values():
        if isinstance(value, dict):
            return len(value)
    return 0


def _build_report_object_name(
    config: Dict[str, Any],
    batch_ts: Any,
    execution_id: str,
) -> str:
    storage = config["general"]["storage"]
    report_folder = storage["quality_report_folder"]
    batch_dt = datetime.fromisoformat(str(batch_ts))
    year = batch_dt.strftime("%Y")
    month = batch_dt.strftime("%m")
    day = batch_dt.strftime("%d")
    hour = batch_dt.strftime("%H")
    hhmm = batch_dt.strftime("%H%M")
    execution_suffix = execution_id.replace("-", "")[:8]
    return (
        f"{report_folder}/gtfs/year={year}/month={month}/day={day}/hour={hour}/"
        f"quality-report-gtfs_{hhmm}_{execution_suffix}.json"
    )


def build_quality_report_path(
    config: Dict[str, Any],
    batch_ts: Any,
    execution_id: str,
) -> str:
    bucket_name = config["general"]["storage"]["metadata_bucket"]
    object_name = _build_report_object_name(config, batch_ts, execution_id)
    return f"{bucket_name}/{object_name}"


def data_quality_report_to_json(data_quality_report: Dict[str, Any]) -> str:
    return json.dumps(data_quality_report, ensure_ascii=False, indent=2, default=str)


def _build_summary(
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
    rows_failed = sum(
        _count_failed_items(stage.get("error_details", {}))
        for stage in stage_results.values()
    )
    failure_stage_data = stage_results.get(failure_phase, {}) if failure_phase else {}
    return {
        "contract_version": "v1",
        "pipeline": "gtfs",
        "stage": "pipeline",
        "execution_id": execution_id,
        "status": status,
        "failure_phase": failure_phase,
        "failure_message": failure_message,
        "acceptance_rate": 0.0 if status == "FAIL" else 1.0,
        "rows_failed": rows_failed,
        "validated_items_count": validated_items_count,
        "quality_report_path": quality_report_path,
        "relocation_status": failure_stage_data.get("relocation_status"),
        "relocation_error": failure_stage_data.get("relocation_error"),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def build_data_quality_report(
    execution_id: str,
    status: str,
    stage_results: Dict[str, Dict[str, Any]],
    quality_report_path: str,
    failure_phase: Optional[str] = None,
    failure_message: Optional[str] = None,
) -> Dict[str, Any]:
    summary = _build_summary(
        execution_id=execution_id,
        status=status,
        stage_results=stage_results,
        quality_report_path=quality_report_path,
        failure_phase=failure_phase,
        failure_message=failure_message,
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


def save_data_quality_report_to_storage(
    config: Dict[str, Any],
    data_quality_report: Dict[str, Any],
    batch_ts: Any,
    execution_id: str,
    write_fn=write_generic_bytes_to_object_storage,
) -> str:
    storage = config["general"]["storage"]
    connections = config["connections"]
    bucket_name = storage["metadata_bucket"]
    object_name = _build_report_object_name(config, batch_ts, execution_id)
    connection_data = {
        **connections["object_storage"],
        "secure": False,
    }
    write_fn(
        connection_data,
        buffer=data_quality_report_to_json(data_quality_report).encode("utf-8"),
        bucket_name=bucket_name,
        object_name=object_name,
    )
    return f"{bucket_name}/{object_name}"


def create_data_quality_report(
    config: Dict[str, Any],
    execution_id: str,
    stage_results: Dict[str, Dict[str, Any]],
    batch_ts: Optional[Any] = None,
    write_fn=write_generic_bytes_to_object_storage,
) -> Dict[str, Any]:
    batch_ts_value = batch_ts or datetime.now(timezone.utc).isoformat()
    report_path = build_quality_report_path(config, batch_ts_value, execution_id)
    report = build_data_quality_report(
        execution_id=execution_id,
        status="PASS",
        stage_results=stage_results,
        quality_report_path=report_path,
        failure_phase=None,
        failure_message=None,
    )
    save_data_quality_report_to_storage(
        config=config,
        data_quality_report=report,
        batch_ts=batch_ts_value,
        execution_id=execution_id,
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
    write_fn=write_generic_bytes_to_object_storage,
) -> Dict[str, Any]:
    batch_ts_value = batch_ts or datetime.now(timezone.utc).isoformat()
    report_path = build_quality_report_path(config, batch_ts_value, execution_id)
    report = build_data_quality_report(
        execution_id=execution_id,
        status="FAIL",
        stage_results=stage_results,
        quality_report_path=report_path,
        failure_phase=failure_phase,
        failure_message=failure_message,
    )
    save_data_quality_report_to_storage(
        config=config,
        data_quality_report=report,
        batch_ts=batch_ts_value,
        execution_id=execution_id,
        write_fn=write_fn,
    )
    return report
