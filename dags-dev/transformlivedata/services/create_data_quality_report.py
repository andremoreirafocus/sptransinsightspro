from typing import Any, Dict, Tuple, Optional
import json
import logging
from datetime import datetime, timezone
from infra.object_storage import write_generic_bytes_to_object_storage

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def build_quality_summary(
    config: Dict[str, Any],
    execution_id: str,
    logical_date_utc: str,
    source_file: str,
    pass_threshold: float,
    warn_threshold: float,
    batch_ts: Any,
    transform_result: Optional[Dict[str, Any]] = None,
    expectations_result: Optional[Dict[str, Any]] = None,
    status: Optional[str] = None,
    acceptance_rate: Optional[float] = None,
    rows_failed: Optional[int] = None,
    failure_phase: Optional[str] = None,
    failure_message: Optional[str] = None,
    quarantine_save_status: Optional[str] = None,
    quarantine_save_error: Optional[str] = None,
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
        transform_invalid_records = transform_result.get("invalid_positions").shape[0]
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
    generated_at = datetime.now(timezone.utc).isoformat()
    return {
        "contract_version": "v1",
        "pipeline": "transformlivedata",
        "execution_id": execution_id,
        "logical_date_utc": logical_date_utc,
        "source_file": source_file,
        "status": status,
        "failure_phase": failure_phase,
        "failure_message": failure_message,
        "acceptance_rate": acceptance_rate,
        "rows_failed": rows_failed,
        "issue_counts": issue_counts,
        "quality_report_path": build_quality_report_path(config, batch_ts),
        "quarantine_save_status": quarantine_save_status,
        "quarantine_save_error": quarantine_save_error,
        "generated_at_utc": generated_at,
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
        logger.error("Error parsing expectations_result: %s", e)
        raise ValueError(f"Error parsing expectations_result: {e}")
    metrics = transform_result.get("metrics", {})
    issues = transform_result.get("issues", {})
    transformed_records = transform_result.get("positions").shape[0]
    transform_invalid_records = transform_result.get("invalid_positions").shape[0]
    accepted_records = valid_df.shape[0]
    rejected_records = transform_invalid_records
    total_raw_records = metrics.get("total_vehicles_processed", 0)
    rows_failed_total = rejected_records + expectations_summary.get("rows_failed", 0)
    acceptance_rate = (
        (total_raw_records - rows_failed_total) / total_raw_records
        if total_raw_records > 0
        else 0.0
    )
    expectations_with_violations = expectations_summary.get(
        "expectations_with_violations", 0
    )
    expectations_failed_due_to_exceptions = expectations_summary.get(
        "expectations_failed_due_to_exceptions", 0
    )
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
    summary = build_quality_summary(
        config=config,
        execution_id=execution_id,
        logical_date_utc=logical_date_utc,
        source_file=source_file,
        pass_threshold=pass_threshold,
        warn_threshold=warn_threshold,
        batch_ts=batch_ts,
        transform_result=transform_result,
        expectations_result=expectations_result,
        status=status,
        acceptance_rate=acceptance_rate,
        rows_failed=rows_failed_total,
        failure_phase=None,
        failure_message=None,
        quarantine_save_status=quarantine_save_status,
        quarantine_save_error=quarantine_save_error,
    )
    details = {
        "execution_id": execution_id,
        "logical_date_utc": logical_date_utc,
        "source_file": source_file,
        "transformation_row_counts": {
            "raw_records": metrics.get("total_vehicles_processed", 0),
            "transformed_records": transformed_records,
            "accepted_records": accepted_records,
            "rejected_records": rejected_records,
        },
        "transformation_metrics": {
            "total_vehicles_processed": metrics.get("total_vehicles_processed", 0),
            "valid_vehicles": metrics.get("valid_vehicles", 0),
            "invalid_vehicles": metrics.get("invalid_vehicles", 0),
            "expected_vehicles": metrics.get("expected_vehicles", 0),
            "total_lines_processed": metrics.get("total_lines_processed", 0),
        },
        "transformation_issues": {
            "invalid_trips": issues.get("invalid_trips", []),
            "invalid_vehicle_ids": issues.get("invalid_vehicle_ids", []),
            "distance_calculation_errors": len(
                issues.get("distance_calculation_errors", [])
            ),
            "lines_with_invalid_vehicles": issues.get("lines_with_invalid_vehicles", 0),
        },
        "expectations_summary": {
            "total_checks": expectations_summary.get("total_checks", 0),
            "expectations_successful": expectations_summary.get(
                "expectations_successful", 0
            ),
            "expectations_with_violations": expectations_with_violations,
            "expectations_failed_due_to_exceptions": expectations_failed_due_to_exceptions,
            "rows_failed": expectations_summary.get("rows_failed", 0),
            "violation_reasons": expectations_summary.get("violation_reasons", []),
            "exception_reasons": expectations_summary.get("exception_reasons", []),
        },
        "outcome": {
            "status": status,
            "acceptance_rate": acceptance_rate,
            "policy_version": "v1",
        },
        "artifacts": {
            "quality_report_path": build_quality_report_path(config, batch_ts),
            "quarantine_path": build_quarantine_path(config, batch_ts),
            "colum lineage": transform_result.get("lineage", {}),
        },
    }
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
    validation_report = format_data_quality_report(data_quality_report)
    logger.info(validation_report)
    save_data_quality_report_to_storage(
        config, data_quality_report, transform_result["batch_ts"]
    )
    return data_quality_report


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
) -> Dict[str, Any]:
    batch_ts_value = batch_ts or logical_date_utc
    summary = build_quality_summary(
        config=config,
        execution_id=execution_id,
        logical_date_utc=logical_date_utc,
        source_file=source_file,
        pass_threshold=pass_threshold,
        warn_threshold=warn_threshold,
        batch_ts=batch_ts_value,
        transform_result=transform_result,
        expectations_result=expectations_result,
        status="FAIL",
        failure_phase=failure_phase,
        failure_message=failure_message,
        quarantine_save_status=quarantine_save_status,
        quarantine_save_error=quarantine_save_error,
    )
    if transform_result is not None and expectations_result is not None:
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
                "quality_report_path": build_quality_report_path(
                    config, batch_ts_value
                ),
                "quarantine_path": build_quarantine_path(config, batch_ts_value),
                "colum lineage": {},
            },
        }
        data_quality_report = {
            "summary": summary,
            "details": details,
        }
    save_data_quality_report_to_storage(config, data_quality_report, batch_ts_value)
    return data_quality_report


def build_quality_report_path(config: Dict[str, Any], batch_ts: Any) -> str:
    def get_config(config: Dict[str, Any]) -> Tuple[str, str]:
        general = config["general"]
        storage = general["storage"]
        bucket_name = storage["metadata_bucket"]
        report_folder = storage["quality_report_folder"]
        return bucket_name, report_folder

    bucket_name, report_folder = get_config(config)
    batch_ts = datetime.fromisoformat(str(batch_ts))
    year = batch_ts.strftime("%Y")
    month = batch_ts.strftime("%m")
    day = batch_ts.strftime("%d")
    hour = batch_ts.strftime("%H")
    hhmm = batch_ts.strftime("%H%M")
    prefix = f"{report_folder}/transformlivedata/year={year}/month={month}/day={day}/hour={hour}/"
    return f"{bucket_name}/{prefix}quality-report-positions_{hhmm}.json"


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


def format_data_quality_report(data_quality_report: Dict[str, Any]) -> str:
    try:
        summary = data_quality_report.get("summary", {})
        details = data_quality_report.get("details", data_quality_report)
        row_counts = details.get("transformation_row_counts", {})
        transform_metrics = details.get("transformation_metrics", {})
        transform_issues = details.get("transformation_issues", {})
        expectations_summary = details.get("expectations_summary", {})
        outcome = details.get("outcome", {})
        execution_id = details.get("execution_id", summary.get("execution_id"))
        logical_date_utc = details.get(
            "logical_date_utc", summary.get("logical_date_utc")
        )
        source_file = details.get("source_file", summary.get("source_file"))
        artifacts = details.get("artifacts", {})
        column_lineage = artifacts.get("colum lineage", {})
        lines = [
            "data_quality_report SUMMARY",
            "-" * 80,
            f"Execution ID: {execution_id}",
            f"Logical Date (UTC): {logical_date_utc}",
            f"Source File: {source_file}",
            "",
            "Record Counts",
            f"- Raw input records: {row_counts.get('raw_records', 0)}",
            f"- Transformed records: {row_counts.get('transformed_records', 0)}",
            f"- Accepted records: {row_counts.get('accepted_records', 0)}",
            f"- Rejected records: {row_counts.get('rejected_records', 0)}",
            "",
            "Transformation Processing Metrics",
            f"- Total vehicles processed: {transform_metrics.get('total_vehicles_processed', 0)}",
            f"- Valid vehicles: {transform_metrics.get('valid_vehicles', 0)}",
            f"- Invalid vehicles: {transform_metrics.get('invalid_vehicles', 0)}",
            f"- Expected vehicles: {transform_metrics.get('expected_vehicles', 0)}",
            f"- Lines processed: {transform_metrics.get('total_lines_processed', 0)}",
            "",
            "Transformation Processing Issues",
            f"- Invalid trips: {len(transform_issues.get('invalid_trips', []))} - {transform_issues.get('invalid_trips', [])}",
            f"- Invalid vehicle IDs: {len(transform_issues.get('invalid_vehicle_ids', []))} - {transform_issues.get('invalid_vehicle_ids', [])}",
            f"- Distance calculation errors: {transform_issues.get('distance_calculation_errors', 0)}",
            f"- Lines with invalid vehicles: {transform_issues.get('lines_with_invalid_vehicles', 0)}",
            "",
            "Post Transformation Validation Summary",
            f"- Total checks: {expectations_summary.get('total_checks', 0)}",
            f"- Expectations successful: {expectations_summary.get('expectations_successful', 0)}",
            f"- Expectations with violations: {expectations_summary.get('expectations_with_violations', 0)}",
        ]
        if expectations_summary.get("expectations_failed_due_to_exceptions", 0) > 0:
            lines.append(
                f"- Expectations failed due to exceptions: {expectations_summary.get('expectations_failed_due_to_exceptions', 0)}"
            )
        lines.append(f"- Records failed: {summary.get('rows_failed', 0)}")
        if expectations_summary.get("violation_reasons"):
            lines.append(
                f"- Expectation violation reasons: {expectations_summary.get('violation_reasons')}"
            )
        if expectations_summary.get("exception_reasons"):
            lines.append(
                f"- Expectation exception reasons: {expectations_summary.get('exception_reasons')}"
            )
        lines.extend(
            [
                f"- Colum lineage: {column_lineage}",
                "",
                "Outcome",
                f"- Status: {outcome.get('status', summary.get('status'))}",
                f"- Acceptance rate: {outcome.get('acceptance_rate', summary.get('acceptance_rate', 0.0)) * 100:.2f}%",
                f"- Policy version: {outcome.get('policy_version', 'v1')}",
            ]
        )
        return "\n".join(lines)
    except Exception as e:
        logger.error("Error parsing data_quality_report: %s", e)
        raise ValueError(f"Error parsing data_quality_report: {e}")


def data_quality_report_to_json(data_quality_report: Dict[str, Any]) -> str:
    return json.dumps(data_quality_report, ensure_ascii=False, indent=2, default=str)


def write_data_quality_report_json(
    data_quality_report: Dict[str, Any], output_path: str
) -> None:
    with open(output_path, "w") as f:
        f.write(data_quality_report_to_json(data_quality_report))


def save_data_quality_report_to_storage(
    config: Dict[str, Any],
    data_quality_report: Dict[str, Any],
    batch_ts: Any,
    write_fn=write_generic_bytes_to_object_storage,
) -> None:
    def get_config(config: Dict[str, Any]) -> Tuple[str, str, str, Dict[str, Any]]:
        general = config["general"]
        connections = config["connections"]
        storage = general["storage"]
        bucket_name = storage["metadata_bucket"]
        report_folder = storage["quality_report_folder"]
        app_folder = storage["app_folder"]
        connection_data = {
            **connections["object_storage"],
            "secure": False,
        }
        return bucket_name, report_folder, app_folder, connection_data

    bucket_name, report_folder, app_folder, connection_data = get_config(config)
    batch_ts = datetime.fromisoformat(str(batch_ts))
    year = batch_ts.strftime("%Y")
    month = batch_ts.strftime("%m")
    day = batch_ts.strftime("%d")
    hour = batch_ts.strftime("%H")
    hhmm = batch_ts.strftime("%H%M")
    prefix = f"{report_folder}/transformlivedata/year={year}/month={month}/day={day}/hour={hour}/"
    object_name = f"{prefix}quality-report-positions_{hhmm}.json"
    write_fn(
        connection_data,
        buffer=data_quality_report_to_json(data_quality_report).encode("utf-8"),
        bucket_name=bucket_name,
        object_name=object_name,
    )
