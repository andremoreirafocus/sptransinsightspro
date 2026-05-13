from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional


def build_quality_summary(
    pipeline: str,
    execution_id: str,
    status: Literal["PASS", "WARN", "FAIL"],
    items_failed: int,
    quality_report_path: str,
    acceptance_rate: Optional[float] = None,
    failure_phase: Optional[str] = None,
    failure_message: Optional[str] = None,
    **extra_fields: Any,
) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "contract_version": "v1",
        "pipeline": pipeline,
        "execution_id": execution_id,
        "status": status,
        "failure_phase": failure_phase,
        "failure_message": failure_message,
        "items_failed": items_failed,
        "quality_report_path": quality_report_path,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    if acceptance_rate is not None:
        summary["acceptance_rate"] = acceptance_rate
    summary.update(extra_fields)
    return summary


def create_failure_quality_report(
    pipeline: str,
    execution_id: str,
    failure_phase: str,
    failure_message: str,
    quality_report_path: str,
    details: Optional[Dict[str, Any]] = None,
    **extra_fields: Any,
) -> Dict[str, Any]:
    # Keep compatibility signature with shared reporting module.
    # For extractloadlivedata, reporting is summary-only; `details` is intentionally ignored.
    _ = details
    acceptance_rate = extra_fields.pop("acceptance_rate", 1.0)
    items_failed = extra_fields.pop("items_failed", 1)
    return build_quality_summary(
        pipeline=pipeline,
        execution_id=execution_id,
        status="FAIL",
        acceptance_rate=acceptance_rate,
        items_failed=items_failed,
        quality_report_path=quality_report_path,
        failure_phase=failure_phase,
        failure_message=failure_message,
        **extra_fields,
    )
