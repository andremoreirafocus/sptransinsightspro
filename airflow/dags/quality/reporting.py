import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional

from infra.notifications import send_webhook
from infra.object_storage import write_generic_bytes_to_object_storage

logger = logging.getLogger(__name__)


def build_quality_report_path(
    metadata_bucket: str,
    quality_report_folder: str,
    pipeline_name: str,
    batch_ts: Any,
    filename_suffix: str = "",
    filename_label: Optional[str] = None,
) -> str:
    """Build the partitioned MinIO path for a quality report.

    Args:
        metadata_bucket: Bucket where quality reports are stored.
        quality_report_folder: Top-level folder inside the bucket (e.g. "quality-reports").
        pipeline_name: Pipeline identifier used for the sub-directory.
        batch_ts: ISO-8601 string or datetime used to derive the partition components.
        filename_suffix: Optional suffix appended to the filename before ".json"
                         (e.g. "_exec1234" for the execution-ID shorthand used by GTFS).
        filename_label: Label used in the filename instead of ``pipeline_name`` when
                        the two differ (e.g. "positions" for transformlivedata, whose
                        reports live under the ``transformlivedata/`` directory but are
                        named ``quality-report-positions_{HHMM}.json``).
                        Defaults to ``pipeline_name``.

    Returns:
        Full path as
        "{bucket}/{folder}/{pipeline}/year=.../…/quality-report-{label}_{HHMM}{suffix}.json"
    """
    label = filename_label if filename_label is not None else pipeline_name
    batch_dt = datetime.fromisoformat(str(batch_ts))
    year = batch_dt.strftime("%Y")
    month = batch_dt.strftime("%m")
    day = batch_dt.strftime("%d")
    hour = batch_dt.strftime("%H")
    hhmm = batch_dt.strftime("%H%M")
    prefix = (
        f"{quality_report_folder}/{pipeline_name}/"
        f"year={year}/month={month}/day={day}/hour={hour}/"
    )
    filename = f"quality-report-{label}_{hhmm}{filename_suffix}.json"
    return f"{metadata_bucket}/{prefix}{filename}"


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
    """Build the standardised summary block shared by all pipelines.

    The mandatory fields form the common contract consumed by the alertservice.
    Pipeline-specific fields are merged flat via ``extra_fields``.

    Args:
        pipeline: Pipeline identifier (e.g. "gtfs", "transformlivedata").
        execution_id: UUID for the current pipeline run.
        status: One of "PASS", "WARN", or "FAIL".
        items_failed: Count of items (tables/files/records) that failed validation.
        quality_report_path: Full MinIO path where the report is stored.
        acceptance_rate: Fraction of records that passed all validations (0.0–1.0).
                         Omit for pipelines using all-or-nothing status logic.
        failure_phase: Name of the stage where a hard failure occurred, or None.
        failure_message: Human-readable description of the failure, or None.
        **extra_fields: Additional pipeline-specific fields merged at the top level.

    Returns:
        Dict representing the summary block.
    """
    summary = {
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
    """Build a failure report for use in exception handlers.

    Produces the standard ``{summary, details}`` envelope with status forced to
    "FAIL". When ``details`` is omitted a minimal block is used; callers that
    collected partial pipeline results (e.g. transform metrics before a late-stage
    failure) should pass their own assembled ``details`` dict so that diagnostic
    information is preserved in the report.

    Args:
        pipeline: Pipeline identifier.
        execution_id: UUID for the current pipeline run.
        failure_phase: Stage name where the hard failure occurred.
        failure_message: Human-readable error description.
        quality_report_path: Full MinIO path for the report.
        details: Optional caller-supplied details block. When provided it is used
                 as-is, allowing pipelines to include partial results collected
                 before the failure. When omitted a minimal
                 ``{"execution_id": ..., "status": "FAIL"}`` block is used.
        **extra_fields: Additional pipeline-specific summary fields.

    Returns:
        Dict with keys "summary" and "details".
    """
    summary = build_quality_summary(
        pipeline=pipeline,
        execution_id=execution_id,
        status="FAIL",
        acceptance_rate=0.0,
        items_failed=0,
        quality_report_path=quality_report_path,
        failure_phase=failure_phase,
        failure_message=failure_message,
        **extra_fields,
    )
    resolved_details: Dict[str, Any] = (
        details
        if details is not None
        else {"execution_id": execution_id, "status": "FAIL"}
    )
    return {"summary": summary, "details": resolved_details}


def save_quality_report(
    report: Dict[str, Any],
    path: str,
    connection_data: Dict[str, Any],
    write_fn=write_generic_bytes_to_object_storage,
) -> None:
    """Persist a quality report to object storage.

    The ``path`` format is ``"{bucket}/{object_name}"``; the bucket and object
    name are derived by splitting on the first ``/``.

    Storage failures are re-raised to the caller.

    Args:
        report: Full quality report dict with "summary" and "details" keys.
        path: Full MinIO path as returned by ``build_quality_report_path``.
        connection_data: Object-storage connection dict (endpoint, access_key, secret_key).
        write_fn: Injectable storage writer (default: MinIO implementation).
    """
    bucket_name, object_name = path.split("/", 1)
    payload = json.dumps(report, ensure_ascii=False, indent=2, default=str).encode(
        "utf-8"
    )
    write_fn(
        connection_data,
        buffer=payload,
        bucket_name=bucket_name,
        object_name=object_name,
    )


def save_and_notify_quality_report(
    report: Dict[str, Any],
    path: str,
    connection_data: Dict[str, Any],
    webhook_url: str,
    write_fn=write_generic_bytes_to_object_storage,
) -> None:
    """Persist a quality report to object storage and send a webhook notification.

    The ``path`` format is ``"{bucket}/{object_name}"``; the bucket and object
    name are derived by splitting on the first ``/``.

    Webhook failures are logged and swallowed so that a notification outage
    never blocks the pipeline. Storage failures are re-raised.

    Args:
        report: Full quality report dict with "summary" and "details" keys.
        path: Full MinIO path as returned by ``build_quality_report_path``.
        connection_data: Object-storage connection dict (endpoint, access_key, secret_key).
        webhook_url: Destination URL; pass "disabled", "none", or "null" to skip.
        write_fn: Injectable storage writer (default: MinIO implementation).
    """
    bucket_name, object_name = path.split("/", 1)
    payload = json.dumps(report, ensure_ascii=False, indent=2, default=str).encode(
        "utf-8"
    )
    write_fn(
        connection_data,
        buffer=payload,
        bucket_name=bucket_name,
        object_name=object_name,
    )
    try:
        send_webhook(report["summary"], webhook_url)
    except Exception as exc:
        logger.warning("Quality report webhook notification failed: %s", exc)
