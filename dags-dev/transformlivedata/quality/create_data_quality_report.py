from typing import Dict, Any
import json
from datetime import datetime
from infra.minio_functions import write_generic_bytes_to_minio


def build_uqr(
    config,
    execution_id: str,
    logical_date_utc: str,
    source_file: str,
    transform_result: Dict[str, Any],
    valid_df,
    invalid_df,
    expectations_summary: Dict[str, Any],
    pass_threshold: float,
    warn_threshold: float,
    batch_ts,
) -> Dict[str, Any]:
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
    checks_failed = expectations_summary.get("checks_failed", 0)
    if acceptance_rate >= pass_threshold and checks_failed == 0:
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
            "vehicle_count_discrepancies_per_line": len(
                issues.get("vehicle_count_discrepancies_per_line", [])
            ),
        },
        "expectations_summary": {
            "total_checks": expectations_summary.get("total_checks", 0),
            "checks_failed": expectations_summary.get("checks_failed", 0),
            "rows_failed": expectations_summary.get("rows_failed", 0),
            "failure_reasons": expectations_summary.get("failure_reasons", []),
        },
        "outcome": {
            "status": status,
            "acceptance_rate": acceptance_rate,
            "policy_version": "v1",
        },
        "artifacts": {
            "quality_report_path": build_quality_report_path(config, batch_ts),
            "quarantine_path": build_quarantine_path(config, batch_ts),
        },
    }


def build_quality_report_path(config, batch_ts):
    def get_config(config):
        bucket_name = config["METADATA_BUCKET"]
        report_folder = config["QUALITY_REPORT_FOLDER"]
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


def build_quarantine_path(config, batch_ts):
    def get_config(config):
        bucket_name = config["QUARANTINED_BUCKET"]
        app_folder = config["APP_FOLDER"]
        positions_table_name = config["POSITIONS_TABLE_NAME"]
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


def format_uqr_report(uqr: Dict[str, Any]) -> str:
    row_counts = uqr.get("row_counts", {})
    transform_metrics = uqr.get("transformation_metrics", {})
    transform_issues = uqr.get("transformation_issues", {})
    expectations_summary = uqr.get("expectations_summary", {})
    outcome = uqr.get("outcome", {})
    lines = [
        "UQR SUMMARY",
        "-" * 80,
        f"Execution ID: {uqr.get('execution_id')}",
        f"Logical Date (UTC): {uqr.get('logical_date_utc')}",
        f"Source File: {uqr.get('source_file')}",
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
        f"- Lines with vehicle count discrepancies: {transform_issues.get('vehicle_count_discrepancies_per_line', 0)}",
        "",
        "Post Transformation Validation Summary",
        f"- Total checks: {expectations_summary.get('total_checks', 0)}",
        f"- Checks failed: {expectations_summary.get('checks_failed', 0)}",
        f"- Records failed: {expectations_summary.get('rows_failed', 0)}",
        f"- Failure reasons: {expectations_summary.get('failure_reasons', [])}",
        "",
        "Outcome",
        f"- Status: {outcome.get('status', 'WARN')}",
        f"- Acceptance rate: {outcome.get('acceptance_rate', 0.0) * 100:.2f}%",
        f"- Policy version: {outcome.get('policy_version', 'v1')}",
    ]
    return "\n".join(lines)


def uqr_to_json(uqr: Dict[str, Any]) -> str:
    return json.dumps(uqr, ensure_ascii=False, indent=2, default=str)


def write_uqr_json(uqr: Dict[str, Any], output_path: str) -> None:
    with open(output_path, "w") as f:
        f.write(uqr_to_json(uqr))


def save_uqr_to_storage(config, uqr, batch_ts):
    def get_config(config):
        bucket_name = config["METADATA_BUCKET"]
        report_folder = config["QUALITY_REPORT_FOLDER"]
        app_folder = config["APP_FOLDER"]
        connection_data = {
            "minio_endpoint": config["MINIO_ENDPOINT"],
            "access_key": config["ACCESS_KEY"],
            "secret_key": config["SECRET_KEY"],
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
        buffer=uqr_to_json(uqr).encode("utf-8"),
        bucket_name=bucket_name,
        object_name=object_name,
    )
