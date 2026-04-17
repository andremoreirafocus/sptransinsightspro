from datetime import datetime, timezone
from typing import Any, Dict


def create_failure_quality_report(
    execution_id: str,
    failure_phase: str,
    failure_message: str,
    validation_result: Dict[str, Any] | None = None,
    quarantine_save_status: str | None = None,
    quarantine_save_error: str | None = None,
) -> Dict[str, Any]:
    """Build an in-memory failure report for GTFS stage-1 orchestration.

    Matches transformlivedata failure summary style with applicable fields,
    without persisting the report.
    """
    validation_result = validation_result or {}
    errors_by_file = validation_result.get("errors_by_file", {})

    summary = {
        "contract_version": "v1",
        "pipeline": "gtfs",
        "stage": "extract_load_files",
        "execution_id": execution_id,
        "status": "FAIL",
        "failure_phase": failure_phase,
        "failure_message": failure_message,
        "rows_failed": len(errors_by_file),
        "acceptance_rate": 0.0,
        "quarantine_save_status": quarantine_save_status,
        "quarantine_save_error": quarantine_save_error,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    details = {
        "validated_files_count": validation_result.get("validated_files_count", 0),
        "errors_by_file": errors_by_file,
    }

    return {
        "summary": summary,
        "details": details,
    }
