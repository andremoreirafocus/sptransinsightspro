import logging
from typing import Any, Dict, Optional

from infra.object_storage import write_generic_bytes_to_object_storage
from quality.reporting import (
    build_quality_report_path,
    build_quality_summary,
    save_quality_report,
)

logger = logging.getLogger(__name__)

PIPELINE_NAME = "refinedfinishedtrips"


def _count_failed_checks(positions_result: Dict[str, Any]) -> int:
    return sum(
        1 for c in positions_result.get("checks", []) if c.get("status") == "FAIL"
    )


def build_quality_report(
    execution_id: str,
    positions_result: Dict[str, Any],
    quality_report_path: str,
    status: str,
    failure_phase: Optional[str] = None,
    failure_message: Optional[str] = None,
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
        "phases": {
            "positions": positions_result,
        },
        "artifacts": {"quality_report_path": quality_report_path},
    }
    return {"summary": summary, "details": details}


def create_quality_report(
    config: Dict[str, Any],
    execution_id: str,
    run_ts: Any,
    positions_result: Dict[str, Any],
    write_fn=write_generic_bytes_to_object_storage,
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
    write_fn=write_generic_bytes_to_object_storage,
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
    )
    save_quality_report(
        report=report,
        path=report_path,
        connection_data=connection_data,
        write_fn=write_fn,
    )
    logger.info(f"Failure quality report saved to {report_path}.")
    return report
