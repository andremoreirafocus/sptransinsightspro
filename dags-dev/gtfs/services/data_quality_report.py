from datetime import datetime, timezone
from typing import Any, Dict


def _count_failed_items(error_details: Dict[str, Any]) -> int:
    for value in error_details.values():
        if isinstance(value, dict):
            return len(value)
    return 0


def create_failure_quality_report(
    stage: str,
    execution_id: str,
    failure_phase: str,
    failure_message: str,
    error_details: Dict[str, Any] | None = None,
    validated_items_count: int | None = None,
    quarantine_save_status: str | None = None,
    quarantine_save_error: str | None = None,
) -> Dict[str, Any]:
    """Build an in-memory failure report for GTFS orchestration.

    Matches transformlivedata failure summary style with applicable fields,
    without persisting the report.
    """
    error_details = error_details or {}
    rows_failed = _count_failed_items(error_details)

    summary = {
        "contract_version": "v1",
        "pipeline": "gtfs",
        "stage": stage,
        "execution_id": execution_id,
        "status": "FAIL",
        "failure_phase": failure_phase,
        "failure_message": failure_message,
        "rows_failed": rows_failed,
        "acceptance_rate": 0.0,
        "quarantine_save_status": quarantine_save_status,
        "quarantine_save_error": quarantine_save_error,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    details = {
        "validated_items_count": validated_items_count or 0,
        "error_details": error_details,
    }

    return {
        "summary": summary,
        "details": details,
    }
