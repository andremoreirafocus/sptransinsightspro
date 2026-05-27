from typing import Any, Callable, Dict, Optional, Tuple
from datetime import datetime
import pandas as pd
from infra.object_storage import write_generic_bytes_to_object_storage
from observability.structured_event_logger import get_structured_logger
from quality.reporting import build_quality_summary, build_quality_report_path, save_quality_report

structured_logger = get_structured_logger(logger_name=__name__)


def _save_quality_report_to_storage(
    *,
    config: Dict[str, Any],
    report: Dict[str, Any],
    write_fn: Callable[..., Any],
) -> None:
    connection_data = {
        **config["connections"]["object_storage"],
        "secure": False,
    }
    save_quality_report(
        report=report,
        path=report["summary"]["quality_report_path"],
        connection_data=connection_data,
        write_fn=write_fn,
    )


def _compute_partial_metrics(
    transform_result: Optional[Dict[str, Any]],
    expectations_result: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    record_counts: Dict[str, Any] = {}
    transformation_processing_metrics: Dict[str, Any] = {}
    transformation_processing_issues: Dict[str, Any] = {}
    post_transformation_validation_summary: Dict[str, Any] = {}

    if transform_result is not None:
        positions_df = transform_result.get("positions")
        if positions_df is not None:
            record_counts["transformed_records"] = positions_df.shape[0]
        invalid_positions = transform_result.get("invalid_positions")
        if invalid_positions is not None:
            record_counts["rejected_records"] = invalid_positions.shape[0]
        metrics = transform_result.get("metrics")
        if metrics is not None:
            record_counts["raw_input_records"] = metrics["total_vehicles_processed"]
            transformation_processing_metrics = {
                "total_vehicles_processed": metrics["total_vehicles_processed"],
                "valid_vehicles": metrics["valid_vehicles"],
                "invalid_vehicles": metrics["invalid_vehicles"],
                "expected_vehicles": metrics["expected_vehicles"],
                "lines_processed": metrics["total_lines_processed"],
            }
        issues = transform_result.get("issues")
        if issues is not None:
            transformation_processing_issues = {
                "invalid_trips_count": len(issues["invalid_trips"]),
                "invalid_vehicle_ids_count": len(issues["invalid_vehicle_ids"]),
                "distance_calculation_errors_count": len(issues["distance_calculation_errors"]),
                "lines_with_invalid_vehicles": issues["lines_with_invalid_vehicles"],
            }

    if expectations_result is not None:
        valid_df = expectations_result.get("valid_df")
        if valid_df is not None:
            record_counts["accepted_records"] = valid_df.shape[0]
        expectations_summary = expectations_result.get("expectations_summary")
        if expectations_summary is not None:
            post_transformation_validation_summary = {
                "total_checks": expectations_summary["total_checks"],
                "expectations_successful": expectations_summary["expectations_successful"],
                "expectations_with_violations": expectations_summary["expectations_with_violations"],
                "records_failed": expectations_summary["rows_failed"],
            }

    return {
        "record_counts": record_counts,
        "transformation_processing_metrics": transformation_processing_metrics,
        "transformation_processing_issues": transformation_processing_issues,
        "post_transformation_validation_summary": post_transformation_validation_summary,
    }


def _build_quality_details(
    execution_id: str,
    logical_date_utc: str,
    source_file: str,
    transform_result: Dict[str, Any],
    valid_df: Any,
    expectations_summary: Dict[str, Any],
    computed_metrics: Dict[str, Any],
    quality_report_path: str,
    batch_ts: Any,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    metrics = transform_result.get("metrics", {})
    issues = transform_result.get("issues", {})
    total_vehicles = metrics.get("total_vehicles_processed", 0)
    positions_df = transform_result.get("positions")
    if positions_df is None:
        positions_df = pd.DataFrame()
    invalid_positions_df = transform_result.get("invalid_positions")
    if invalid_positions_df is None:
        invalid_positions_df = pd.DataFrame()
    return {
        "execution_id": execution_id,
        "logical_date_utc": logical_date_utc,
        "source_file": source_file,
        "transformation_row_counts": {
            "raw_records": total_vehicles,
            "transformed_records": positions_df.shape[0],
            "accepted_records": valid_df.shape[0],
            "rejected_records": invalid_positions_df.shape[0],
        },
        "transformation_metrics": {
            "total_vehicles_processed": total_vehicles,
            "valid_vehicles": metrics.get("valid_vehicles", 0),
            "invalid_vehicles": metrics.get("invalid_vehicles", 0),
            "expected_vehicles": metrics.get("expected_vehicles", 0),
            "total_lines_processed": metrics.get("total_lines_processed", 0),
        },
        "transformation_issues": {
            "invalid_trips": issues.get("invalid_trips", []),
            "invalid_vehicle_ids": issues.get("invalid_vehicle_ids", []),
            "number_of_distance_calculation_errors": len(issues.get("distance_calculation_errors", [])),
            "lines_with_invalid_vehicles": issues.get("lines_with_invalid_vehicles", 0),
        },
        "expectations_summary": {
            **expectations_summary,
            "number_of_distance_calculation_errors": len(
                issues.get("distance_calculation_errors", [])
            ),
        },
        "outcome": {
            "status": computed_metrics["status"],
            "acceptance_rate": computed_metrics["acceptance_rate"],
            "policy_version": "v1",
        },
        "artifacts": {
            "quality_report_path": quality_report_path,
            "quarantine_path": build_quarantine_path(config, batch_ts),
            "column_lineage": transform_result.get("lineage", {}),
        },
    }


def _compute_quality_metrics(
    transform_result: Optional[Dict[str, Any]] = None,
    expectations_result: Optional[Dict[str, Any]] = None,
    pass_threshold: float = 1.0,
    warn_threshold: float = 0.98,
    rows_failed: Optional[int] = None,
    acceptance_rate: Optional[float] = None,
    status: Optional[str] = None,
    failure_phase: Optional[str] = None,
    failure_message: Optional[str] = None,
) -> Dict[str, Any]:
    expectations_with_violations = 0
    expectations_failed_due_to_exceptions = 0
    if expectations_result is not None:
        expectations_summary = expectations_result.get("expectations_summary", {})
        expectations_with_violations = expectations_summary.get(
            "expectations_with_violations", 0
        )
        expectations_failed_due_to_exceptions = expectations_summary.get(
            "expectations_failed_due_to_exceptions", 0
        )

    if transform_result is not None and expectations_result is not None:
        invalid_df = expectations_result.get("invalid_df")
        transform_invalid_positions = transform_result.get("invalid_positions")
        if transform_invalid_positions is None:
            transform_invalid_positions = pd.DataFrame()
        transform_invalid_records = transform_invalid_positions.shape[0]
        gx_invalid_records = 0 if invalid_df is None else invalid_df.shape[0]
        total_raw_records = transform_result.get("metrics", {}).get(
            "total_vehicles_processed", 0
        )
        if rows_failed is None:
            rows_failed = transform_invalid_records + gx_invalid_records
        if acceptance_rate is None:
            acceptance_rate = (
                (total_raw_records - rows_failed) / total_raw_records
                if total_raw_records > 0
                else 0.0
            )
        if status is None:
            if (
                acceptance_rate >= pass_threshold
                and expectations_with_violations == 0
                and expectations_failed_due_to_exceptions == 0
            ):
                status = "PASS"
            elif acceptance_rate >= warn_threshold:
                status = "WARN"
            else:
                status = "FAIL"
    if status is None:
        status = "FAIL" if failure_phase or failure_message else "WARN"
    if acceptance_rate is None:
        acceptance_rate = 0.0
    if rows_failed is None:
        rows_failed = 0
    issues = transform_result.get("issues", {}) if transform_result is not None else {}
    issue_counts = {
        "invalid_trips": len(issues.get("invalid_trips", [])),
        "invalid_vehicle_ids": len(issues.get("invalid_vehicle_ids", [])),
        "distance_calculation_errors": len(
            issues.get("distance_calculation_errors", [])
        ),
        "lines_with_invalid_vehicles": issues.get("lines_with_invalid_vehicles", 0),
    }
    return {
        "status": status,
        "rows_failed": rows_failed,
        "acceptance_rate": acceptance_rate,
        "issue_counts": issue_counts,
    }


def build_data_quality_report(
    config: Dict[str, Any],
    execution_id: str,
    logical_date_utc: str,
    source_file: str,
    transform_result: Dict[str, Any],
    expectations_result: Dict[str, Any],
    pass_threshold: float,
    warn_threshold: float,
    batch_ts: Any,
    quarantine_save_status: Optional[str] = None,
    quarantine_save_error: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        valid_df = expectations_result["valid_df"]
        expectations_summary = expectations_result["expectations_summary"]
    except Exception as e:
        structured_logger.error(
            event="quality_report_build_failed",
            message="Error parsing expectations_result",
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        raise ValueError(f"Error parsing expectations_result: {e}") from e
    computed_metrics = _compute_quality_metrics(
        transform_result=transform_result,
        expectations_result=expectations_result,
        pass_threshold=pass_threshold,
        warn_threshold=warn_threshold,
    )
    quality_report_path = build_quality_report_path(
        metadata_bucket=config["general"]["storage"]["metadata_bucket"],
        quality_report_folder=config["general"]["storage"]["quality_report_folder"],
        pipeline_name="transformlivedata",
        batch_ts=batch_ts,
        filename_label="positions",
    )
    summary = build_quality_summary(
        pipeline="transformlivedata",
        execution_id=execution_id,
        status=computed_metrics["status"],
        items_failed=computed_metrics["rows_failed"],
        quality_report_path=quality_report_path,
        acceptance_rate=computed_metrics["acceptance_rate"],
        failure_phase=None,
        failure_message=None,
        logical_date_utc=logical_date_utc,
        source_file=source_file,
        issue_counts=computed_metrics["issue_counts"],
        quarantine_save_status=quarantine_save_status,
        quarantine_save_error=quarantine_save_error,
    )
    details = _build_quality_details(
        execution_id=execution_id,
        logical_date_utc=logical_date_utc,
        source_file=source_file,
        transform_result=transform_result,
        valid_df=valid_df,
        expectations_summary=expectations_summary,
        computed_metrics=computed_metrics,
        quality_report_path=quality_report_path,
        batch_ts=batch_ts,
        config=config,
    )
    return {
        "summary": summary,
        "details": details,
    }


def create_data_quality_report(
    config: Dict[str, Any],
    execution_id: str,
    logical_date_utc: str,
    source_file: str,
    transform_result: Dict[str, Any],
    expectations_result: Dict[str, Any],
    pass_threshold: float = 1.0,
    warn_threshold: float = 0.980,
    quarantine_save_status: Optional[str] = None,
    quarantine_save_error: Optional[str] = None,
    write_fn: Callable[..., Any] = write_generic_bytes_to_object_storage,
) -> Dict[str, Any]:
    data_quality_report = build_data_quality_report(
        config=config,
        execution_id=execution_id,
        logical_date_utc=logical_date_utc,
        source_file=source_file,
        transform_result=transform_result,
        expectations_result=expectations_result,
        pass_threshold=pass_threshold,
        warn_threshold=warn_threshold,
        batch_ts=transform_result["batch_ts"],
        quarantine_save_status=quarantine_save_status,
        quarantine_save_error=quarantine_save_error,
    )
    report_metrics = create_data_quality_metrics(data_quality_report)
    summary = data_quality_report["summary"]
    _save_quality_report_to_storage(
        config=config,
        report=data_quality_report,
        write_fn=write_fn,
    )
    return {
        **report_metrics,
        "summary_status": summary["status"],
        "quality_report_path": summary["quality_report_path"],
    }


def create_failure_quality_report(
    config: Dict[str, Any],
    execution_id: str,
    logical_date_utc: str,
    source_file: str,
    failure_phase: str,
    failure_message: str,
    pass_threshold: float = 1.0,
    warn_threshold: float = 0.980,
    batch_ts: Optional[Any] = None,
    transform_result: Optional[Dict[str, Any]] = None,
    expectations_result: Optional[Dict[str, Any]] = None,
    quarantine_save_status: Optional[str] = None,
    quarantine_save_error: Optional[str] = None,
    write_fn: Callable[..., Any] = write_generic_bytes_to_object_storage,
) -> Dict[str, Any]:
    batch_ts_value = batch_ts or logical_date_utc
    quality_report_path = build_quality_report_path(
        metadata_bucket=config["general"]["storage"]["metadata_bucket"],
        quality_report_folder=config["general"]["storage"]["quality_report_folder"],
        pipeline_name="transformlivedata",
        batch_ts=batch_ts_value,
        filename_label="positions",
    )
    summary = build_quality_summary(
        pipeline="transformlivedata",
        execution_id=execution_id,
        status="FAIL",
        items_failed=0,
        quality_report_path=quality_report_path,
        acceptance_rate=0.0,
        failure_phase=failure_phase,
        failure_message=failure_message,
        logical_date_utc=logical_date_utc,
        source_file=source_file,
        issue_counts={},
        quarantine_save_status=quarantine_save_status,
        quarantine_save_error=quarantine_save_error,
    )
    has_full_results = transform_result is not None and expectations_result is not None

    if has_full_results:
        assert transform_result is not None
        assert expectations_result is not None
        data_quality_report = build_data_quality_report(
            config=config,
            execution_id=execution_id,
            logical_date_utc=logical_date_utc,
            source_file=source_file,
            transform_result=transform_result,
            expectations_result=expectations_result,
            pass_threshold=pass_threshold,
            warn_threshold=warn_threshold,
            batch_ts=batch_ts_value,
            quarantine_save_status=quarantine_save_status,
            quarantine_save_error=quarantine_save_error,
        )
        data_quality_report["summary"] = summary
        report_metrics = create_data_quality_metrics(data_quality_report)
    else:
        details = {
            "execution_id": execution_id,
            "logical_date_utc": logical_date_utc,
            "source_file": source_file,
            "transformation_row_counts": {},
            "transformation_metrics": {},
            "transformation_issues": {},
            "expectations_summary": {},
            "outcome": {"status": "FAIL"},
            "artifacts": {
                "quality_report_path": quality_report_path,
                "quarantine_path": build_quarantine_path(config, batch_ts_value),
                "column_lineage": {},
            },
        }
        data_quality_report = {
            "summary": summary,
            "details": details,
        }
        report_metrics = _compute_partial_metrics(transform_result, expectations_result)

    _save_quality_report_to_storage(
        config=config,
        report=data_quality_report,
        write_fn=write_fn,
    )
    return {
        **report_metrics,
        "summary_status": "FAIL",
        "quality_report_path": quality_report_path,
        "failure_phase": failure_phase,
        "failure_message": failure_message,
    }


def build_quarantine_path(config: Dict[str, Any], batch_ts: Any) -> str:
    def get_config(config: Dict[str, Any]) -> Tuple[str, str, str]:
        general = config["general"]
        storage = general["storage"]
        tables = general["tables"]
        bucket_name = storage["quarantined_bucket"]
        app_folder = storage["app_folder"]
        positions_table_name = tables["positions_table_name"]
        return bucket_name, app_folder, positions_table_name

    bucket_name, app_folder, positions_table_name = get_config(config)
    batch_ts = datetime.fromisoformat(str(batch_ts))
    year = batch_ts.strftime("%Y")
    month = batch_ts.strftime("%m")
    day = batch_ts.strftime("%d")
    hour = batch_ts.strftime("%H")
    hhmm = batch_ts.strftime("%H%M")
    prefix = f"{app_folder}/{positions_table_name}/year={year}/month={month}/day={day}/hour={hour}/"
    return f"{bucket_name}/{prefix}positions_{hhmm}_*.parquet"


def create_data_quality_metrics(data_quality_report: Dict[str, Any]) -> Dict[str, Any]:
    summary = data_quality_report["summary"]
    details = data_quality_report["details"]
    row_counts = details["transformation_row_counts"]
    transform_metrics = details["transformation_metrics"]
    transform_issues = details["transformation_issues"]
    expectations_summary = details["expectations_summary"]
    return {
        "record_counts": {
            "raw_input_records": row_counts["raw_records"],
            "transformed_records": row_counts["transformed_records"],
            "accepted_records": row_counts["accepted_records"],
            "rejected_records": row_counts["rejected_records"],
        },
        "transformation_processing_metrics": {
            "total_vehicles_processed": transform_metrics["total_vehicles_processed"],
            "valid_vehicles": transform_metrics["valid_vehicles"],
            "invalid_vehicles": transform_metrics["invalid_vehicles"],
            "expected_vehicles": transform_metrics["expected_vehicles"],
            "lines_processed": transform_metrics["total_lines_processed"],
        },
        "transformation_processing_issues": {
            "invalid_trips_count": len(transform_issues["invalid_trips"]),
            "invalid_vehicle_ids_count": len(transform_issues["invalid_vehicle_ids"]),
            "distance_calculation_errors_count": transform_issues[
                "number_of_distance_calculation_errors"
            ],
            "lines_with_invalid_vehicles": transform_issues[
                "lines_with_invalid_vehicles"
            ],
        },
        "post_transformation_validation_summary": {
            "total_checks": expectations_summary["total_checks"],
            "expectations_successful": expectations_summary["expectations_successful"],
            "expectations_with_violations": expectations_summary[
                "expectations_with_violations"
            ],
            "records_failed": summary["items_failed"],
            "gx_records_failures": expectations_summary["rows_failed"],
        },
    }


