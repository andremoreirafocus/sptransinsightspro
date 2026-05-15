from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional

MAX_CORRELATION_IDS_IN_REPORT = 50


def _dedup_keep_order(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _prepare_correlation_ids_payload(
    worked_correlation_ids: list[str],
    *,
    max_items: int = MAX_CORRELATION_IDS_IN_REPORT,
) -> Dict[str, Any]:
    deduped_ids = _dedup_keep_order(worked_correlation_ids)
    correlation_ids_count = len(deduped_ids)
    correlation_ids = deduped_ids[:max_items]
    correlation_ids_truncated = correlation_ids_count > len(correlation_ids)
    return {
        "correlation_ids": correlation_ids,
        "correlation_ids_count": correlation_ids_count,
        "correlation_ids_truncated": correlation_ids_truncated,
    }


def build_execution_report_metadata(
    *,
    execution_seconds: float,
    items_total: int,
    items_failed: int,
    retries_seen: int,
    worked_correlation_ids: list[str],
    phase_metrics: Optional[Dict[str, Dict[str, int]]] = None,
    phase_durations: Optional[Dict[str, float]] = None,
    logical_datetime: Optional[str] = None,
) -> Dict[str, Any]:
    correlation_payload = _prepare_correlation_ids_payload(worked_correlation_ids)
    metadata: Dict[str, Any] = {
        "execution_seconds": execution_seconds,
        "items_total": items_total,
        "items_failed": items_failed,
        "retries_seen": retries_seen,
        "correlation_ids": correlation_payload["correlation_ids"],
        "correlation_ids_count": correlation_payload["correlation_ids_count"],
        "correlation_ids_truncated": correlation_payload["correlation_ids_truncated"],
    }
    if phase_metrics is not None:
        metadata["phase_metrics"] = phase_metrics
    if phase_durations is not None:
        metadata["phase_durations"] = phase_durations
    if logical_datetime is not None:
        metadata["logical_datetime"] = logical_datetime
    return metadata


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
