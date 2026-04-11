from typing import Any, Dict, Tuple
import json
import logging
from datetime import datetime
from infra.minio_functions import write_generic_bytes_to_minio

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


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
) -> Dict[str, Any]:
    try:
        valid_df = expectations_result["valid_df"]
        invalid_df = expectations_result["invalid_df"]
        expectations_summary = expectations_result["expectations_summary"]
    except Exception as e:
        logger.error("Error parsing expectations_result: %s", e)
        raise ValueError(f"Error parsing expectations_result: {e}")
    metrics = transform_result.get("metrics", {})
    issues = transform_result.get("issues", {})
    transformed_records = transform_result.get("positions").shape[0]
    transform_invalid_records = transform_result.get("invalid_positions").shape[0]
    gx_invalid_records = 0 if invalid_df is None else invalid_df.shape[0]
    accepted_records = valid_df.shape[0]
    rejected_records = transform_invalid_records + gx_invalid_records
    acceptance_rate = (
        accepted_records / transformed_records if transformed_records > 0 else 0.0
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
    return {
        "execution_id": execution_id,
        "logical_date_utc": logical_date_utc,
        "source_file": source_file,
        "row_counts": {
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
            "lines_with_invalid_vehicles": issues.get(
                "lines_with_invalid_vehicles", 0
            ),
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


def create_data_quality_report(
    config: Dict[str, Any],
    execution_id: str,
    logical_date_utc: str,
    source_file: str,
    transform_result: Dict[str, Any],
    expectations_result: Dict[str, Any],
    pass_threshold: float = 1.0,
    warn_threshold: float = 0.980,
) -> None:
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
    )
    validation_report = format_data_quality_report(data_quality_report)
    logger.info(validation_report)
    save_data_quality_report_to_storage(
        config, data_quality_report, transform_result["batch_ts"]
    )


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
        row_counts = data_quality_report["row_counts"]
        transform_metrics = data_quality_report["transformation_metrics"]
        transform_issues = data_quality_report["transformation_issues"]
        expectations_summary = data_quality_report["expectations_summary"]
        outcome = data_quality_report["outcome"]
        execution_id = data_quality_report["execution_id"]
        logical_date_utc = data_quality_report["logical_date_utc"]
        source_file = data_quality_report["source_file"]
        artifacts = data_quality_report["artifacts"]
        column_lineage = artifacts["colum lineage"]
        lines = [
            "data_quality_report SUMMARY",
            "-" * 80,
            f"Execution ID: {execution_id}",
            f"Logical Date (UTC): {logical_date_utc}",
            f"Source File: {source_file}",
            "",
            "Record Counts",
            f"- Raw input records: {row_counts['raw_records']}",
            f"- Transformed records: {row_counts['transformed_records']}",
            f"- Accepted records: {row_counts['accepted_records']}",
            f"- Rejected records: {row_counts['rejected_records']}",
            "",
            "Transformation Processing Metrics",
            f"- Total vehicles processed: {transform_metrics['total_vehicles_processed']}",
            f"- Valid vehicles: {transform_metrics['valid_vehicles']}",
            f"- Invalid vehicles: {transform_metrics['invalid_vehicles']}",
            f"- Expected vehicles: {transform_metrics['expected_vehicles']}",
            f"- Lines processed: {transform_metrics['total_lines_processed']}",
            "",
            "Transformation Processing Issues",
            f"- Invalid trips: {len(transform_issues['invalid_trips'])} - {transform_issues['invalid_trips']}",
            f"- Invalid vehicle IDs: {len(transform_issues['invalid_vehicle_ids'])} - {transform_issues['invalid_vehicle_ids']}",
            f"- Distance calculation errors: {transform_issues['distance_calculation_errors']}",
            f"- Lines with invalid vehicles: {transform_issues['lines_with_invalid_vehicles']}",
            "",
            "Post Transformation Validation Summary",
            f"- Total checks: {expectations_summary['total_checks']}",
            f"- Expectations successful: {expectations_summary['expectations_successful']}",
            f"- Expectations with violations: {expectations_summary['expectations_with_violations']}",
        ]
        if expectations_summary["expectations_failed_due_to_exceptions"] > 0:
            lines.append(
                f"- Expectations failed due to exceptions: {expectations_summary['expectations_failed_due_to_exceptions']}"
            )
        lines.append(f"- Records failed: {expectations_summary['rows_failed']}")
        if expectations_summary["violation_reasons"]:
            lines.append(
                f"- Expectation violation reasons: {expectations_summary['violation_reasons']}"
            )
        if expectations_summary["exception_reasons"]:
            lines.append(
                f"- Expectation exception reasons: {expectations_summary['exception_reasons']}"
            )
        lines.extend(
            [
                f"- Colum lineage: {column_lineage}",
                "",
                "Outcome",
                f"- Status: {outcome['status']}",
                f"- Acceptance rate: {outcome['acceptance_rate'] * 100:.2f}%",
                f"- Policy version: {outcome['policy_version']}",
            ]
        )
        return "\n".join(lines)
    except Exception as e:
        logger.error("Error parsing data_quality_report: %s", e)
        raise ValueError(f"Error parsing data_quality_report: {e}")


def data_quality_report_to_json(data_quality_report: Dict[str, Any]) -> str:
    return json.dumps(data_quality_report, ensure_ascii=False, indent=2, default=str)


def write_data_quality_report_json(data_quality_report: Dict[str, Any], output_path: str) -> None:
    with open(output_path, "w") as f:
        f.write(data_quality_report_to_json(data_quality_report))


def save_data_quality_report_to_storage(
    config: Dict[str, Any], data_quality_report: Dict[str, Any], batch_ts: Any
) -> None:
    def get_config(config: Dict[str, Any]) -> Tuple[str, str, str, Dict[str, Any]]:
        general = config["general"]
        connections = config["connections"]
        storage = general["storage"]
        bucket_name = storage["metadata_bucket"]
        report_folder = storage["quality_report_folder"]
        app_folder = storage["app_folder"]
        object_storage = connections["object_storage"]
        connection_data = {
            "minio_endpoint": object_storage["endpoint"],
            "access_key": object_storage["access_key"],
            "secret_key": object_storage["secret_key"],
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
    prefix = (
        f"{report_folder}/transformlivedata/year={year}/month={month}/day={day}/hour={hour}/"
    )
    object_name = f"{prefix}quality-report-positions_{hhmm}.json"
    write_generic_bytes_to_minio(
        connection_data,
        buffer=data_quality_report_to_json(data_quality_report).encode("utf-8"),
        bucket_name=bucket_name,
        object_name=object_name,
    )
