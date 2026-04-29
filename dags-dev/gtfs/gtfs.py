from datetime import datetime, timezone
from dataclasses import dataclass, field
import logging
from typing import Any, Callable, Dict, Optional
import uuid

from gtfs.config.gtfs_config_schema import GeneralConfig
from gtfs.services.create_data_quality_report import (
    create_data_quality_report,
    create_failure_quality_report,
)
from gtfs.services.create_save_trip_details import (
    create_trip_details_table_and_fill_missing_data,
)
from gtfs.services.extract_gtfs_files import extract_gtfs_files
from gtfs.services.relocate_staged_trusted_files import relocate_staged_trusted_files
from gtfs.services.save_files_to_raw_storage import save_files_to_raw_storage
from gtfs.services.transforms import transform_and_validate_table
from gtfs.services.validate_raw_gtfs_files import validate_raw_gtfs_files
from gtfs.lineage.trip_details_lineage import (
    get_trip_details_lineage,
    validate_trip_details_lineage,
)
from infra.object_storage import read_file_from_object_storage_to_bytesio
from quality.validate_expectations import validate_expectations
from infra.notifications import send_webhook
from pipeline_configurator.config import get_config
import pandas as pd

logger = logging.getLogger(__name__)

PIPELINE_NAME = "gtfs"


class StageExecutionError(ValueError):
    def __init__(self, stage: str, message: str, stage_result: dict) -> None:
        super().__init__(message)
        self.stage = stage
        self.stage_result = stage_result


@dataclass
class RelocationDetails:
    moved: list[Any] = field(default_factory=list)
    errors: list[Any] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "moved": list(self.moved),
            "errors": list(self.errors),
        }


def load_pipeline_config() -> Dict[str, Any]:
    try:
        return get_config(
            PIPELINE_NAME,
            None,
            GeneralConfig,
            "gtfs_conn",
            "minio_conn",
            None,
        )
    except Exception as e:
        logger.error("Pipeline configuration validation failed: %s", e)
        raise ValueError(f"Pipeline configuration validation failed: {e}")


def send_webhook_from_report(report: dict, pipeline_config: dict, path: str) -> None:
    summary = report.get("summary", {})
    execution_id = summary.get("execution_id")
    status = summary.get("status")
    failure_phase = summary.get("failure_phase")
    webhook_url = (
        pipeline_config.get("general", {}).get("notifications", {}).get("webhook_url")
    )
    normalized_webhook_url = str(webhook_url or "").strip()
    if normalized_webhook_url.lower() in {"", "disabled", "none", "null"}:
        logger.info(
            "Webhook notification disabled (%s): execution_id=%s status=%s failure_phase=%s",
            path,
            execution_id,
            status,
            failure_phase,
        )
        return
    try:
        send_webhook(summary, normalized_webhook_url)
        logger.info(
            "Webhook notification sent (%s): execution_id=%s status=%s failure_phase=%s",
            path,
            execution_id,
            status,
            failure_phase,
        )
    except Exception as e:
        logger.error(
            "Webhook notification failed (%s): execution_id=%s status=%s failure_phase=%s error=%s",
            path,
            execution_id,
            status,
            failure_phase,
            e,
        )


def apply_relocation_result(stage_result: dict, relocation: dict) -> None:
    stage_result["relocation_status"] = relocation.get("status", "FAILED")
    stage_result["relocation_details"] = {
        "moved": relocation.get("moved", []),
        "errors": relocation.get("errors", []),
    }
    if relocation.get("errors"):
        stage_result["relocation_error"] = str(relocation["errors"])
        stage_result["error_details"]["relocation_errors"] = relocation["errors"]


def extract_load_files(run_context: Dict[str, Any], stage_results: Dict[str, Any], write_fn: Optional[Callable[..., Any]] = None) -> Dict[str, Any]:
    pipeline_config = load_pipeline_config()
    stage_result = {
        "status": "FAIL",
        "validated_items_count": 0,
        "error_details": {},
        "relocation_status": "NOT_APPLICABLE",
        "relocation_error": None,
        "relocation_details": RelocationDetails().to_dict(),
    }
    try:
        logger.info("=== EXTRACT & LOAD STAGE: extract_gtfs_files ===")
        files_list = extract_gtfs_files(pipeline_config)
        if not files_list:
            raise ValueError("No GTFS files extracted.")

        logger.info("=== EXTRACT & LOAD STAGE: validate_raw_gtfs_files ===")
        validation_result = validate_raw_gtfs_files(pipeline_config, files_list)
        errors_by_file = validation_result.get("errors_by_file", {})
        stage_result["validated_items_count"] = validation_result.get(
            "validated_files_count", 0
        )
        stage_result["error_details"] = {"errors_by_file": errors_by_file}

        if not validation_result.get("is_valid", False):
            consolidated_reasons = "; ".join(
                [f"{file}:{reasons}" for file, reasons in errors_by_file.items()]
            )
            logger.error("Raw GTFS validation failed: %s", consolidated_reasons)
            try:
                save_files_to_raw_storage(pipeline_config, files_list, failed=True)
                stage_result["relocation_status"] = "SUCCESS"
                stage_result["relocation_details"] = {
                    "moved": [
                        {"scope": "all_extracted_files", "target": "quarantine"}
                    ],
                    "errors": [],
                }
                logger.info("All extracted files saved to quarantine.")
            except Exception as e:
                stage_result["relocation_status"] = "FAILED"
                stage_result["relocation_error"] = str(e)
                stage_result["relocation_details"] = {
                    "moved": [],
                    "errors": [str(e)],
                }
                raise ValueError(
                    f"Validation failed and quarantine save failed: {e}"
                )
            raise ValueError(f"Raw GTFS validation failed: {consolidated_reasons}")

        save_files_to_raw_storage(pipeline_config, files_list)
        stage_result["status"] = "PASS"
        logger.info("EXTRACT & LOAD STAGE completed successfully.")
        stage_results["extract_load_files"] = stage_result
        return stage_results
    except Exception as e:
        if not stage_result.get("error_details"):
            stage_result["error_details"] = {"errors": [str(e)]}
        err = StageExecutionError("extract_load_files", str(e), stage_result)
        handle_unexpected_error(err, run_context, stage_results, write_fn)
        raise err from e


def transform(run_context: Dict[str, Any], stage_results: Dict[str, Any], write_fn: Optional[Callable[..., Any]] = None) -> Dict[str, Any]:
    pipeline_config = load_pipeline_config()
    stage_result = {
        "status": "FAIL",
        "validated_items_count": 0,
        "error_details": {},
        "relocation_status": "NOT_APPLICABLE",
        "relocation_error": None,
        "relocation_details": RelocationDetails().to_dict(),
    }
    table_names = [
        "stops",
        "stop_times",
        "routes",
        "trips",
        "frequencies",
        "calendar",
    ]
    table_results = []
    try:
        logger.info("=== TRANSFORMATION STAGE: transform_and_validate_table ===")
        for table_name in table_names:
            table_results.append(
                transform_and_validate_table(pipeline_config, table_name)
            )
        stage_result["validated_items_count"] = len(table_results)

        errors_by_table = {
            row["table_name"]: row["errors"]
            for row in table_results
            if not row.get("is_valid", False)
        }
        stage_result["error_details"] = {
            "errors_by_table": errors_by_table,
            "table_results": [
                {
                    "table_name": row["table_name"],
                    "staged_written": row["staged_written"],
                }
                for row in table_results
            ],
        }
        staged_results = [row for row in table_results if row.get("staged_written")]
        if errors_by_table:
            relocation = relocate_staged_trusted_files(
                pipeline_config,
                staged_results,
                target="quarantine",
            )
            apply_relocation_result(stage_result, relocation)
            consolidated_reasons = "; ".join(
                [f"{table}:{reasons}" for table, reasons in errors_by_table.items()]
            )
            error_message = f"Validation failures detected: {consolidated_reasons}"
            if stage_result["relocation_error"]:
                error_message = (
                    f"{error_message}; "
                    f"relocation_error:{stage_result['relocation_error']}"
                )
            raise ValueError(error_message)
        relocation = relocate_staged_trusted_files(
            pipeline_config,
            staged_results,
            target="final",
        )
        apply_relocation_result(stage_result, relocation)
        if relocation.get("errors"):
            raise ValueError(
                f"staging_to_final_relocation_error:{stage_result['relocation_error']}"
            )
        stage_result["status"] = "PASS"
        logger.info("TRANSFORMATION STAGE completed successfully.")
        stage_results["transformation"] = stage_result
        return stage_results
    except Exception as e:
        if not stage_result.get("error_details"):
            stage_result["error_details"] = {"errors": [str(e)]}
        err = StageExecutionError("transformation", str(e), stage_result)
        handle_unexpected_error(err, run_context, stage_results, write_fn)
        raise err from e


def create_trip_details(run_context: Dict[str, Any], stage_results: Dict[str, Any], write_fn: Optional[Callable[..., Any]] = None) -> Dict[str, Any]:
    pipeline_config = load_pipeline_config()
    table_name = pipeline_config["general"]["tables"]["trip_details_table_name"]
    staged_result = []
    relocation_target = None
    stage_result = {
        "status": "FAIL",
        "validated_items_count": 1,
        "error_details": {"errors_by_table": {}},
        "artifacts": {"column_lineage": get_trip_details_lineage()},
        "relocation_status": "NOT_APPLICABLE",
        "relocation_error": None,
        "relocation_details": RelocationDetails().to_dict(),
    }
    try:
        logger.info("=== ENRICHMENT STAGE: create_trip_details ===")
        creation_result = create_trip_details_table_and_fill_missing_data(
            pipeline_config
        )
        staging_object_name = creation_result["staging_object_name"]
        staged_result = [
            {
                "table_name": table_name,
                "staging_object_name": staging_object_name,
                "staged_written": True,
            }
        ]

        suite = pipeline_config.get("data_expectations_trip_details")
        if isinstance(suite, dict) and len(suite.get("expectations", [])) > 0:
            logger.info(
                "ENRICHMENT STAGE - Running expectations validation for table '%s'",
                table_name,
            )
            storage = pipeline_config["general"]["storage"]
            trusted_bucket = storage["trusted_bucket"]
            connection_data = {
                **pipeline_config["connections"]["object_storage"],
                "secure": False,
            }
            parquet_buffer = read_file_from_object_storage_to_bytesio(
                connection_data,
                bucket_name=trusted_bucket,
                object_name=staging_object_name,
            )
            trip_details_df = pd.read_parquet(parquet_buffer)
            stage_result["artifacts"]["column_lineage"] = (
                validate_trip_details_lineage(
                    stage_result["artifacts"]["column_lineage"],
                    trip_details_df.columns,
                )
            )
            expectations_result = validate_expectations(trip_details_df, suite)
            summary = expectations_result.get("expectations_summary", {})
            stage_result["expectations_summary"] = summary
            if (
                summary.get("rows_failed", 0) > 0
                or summary.get("expectations_with_violations", 0) > 0
                or summary.get("expectations_failed_due_to_exceptions", 0) > 0
            ):
                stage_result["error_details"]["errors_by_table"][table_name] = [
                    f"gx_validation_failed:{summary}"
                ]
                relocation_target = "quarantine"
                relocation = relocate_staged_trusted_files(
                    pipeline_config,
                    staged_result,
                    target="quarantine",
                )
                apply_relocation_result(stage_result, relocation)
                raise ValueError(
                    f"Validation failures detected: {table_name}:{summary}"
                )
        else:
            logger.info(
                "Validation not required and skipped for table %s",
                table_name,
            )

        relocation_target = "final"
        relocation = relocate_staged_trusted_files(
            pipeline_config,
            staged_result,
            target="final",
        )
        apply_relocation_result(stage_result, relocation)
        if relocation.get("errors"):
            raise ValueError(
                f"staging_to_final_relocation_error:{stage_result['relocation_error']}"
            )
        stage_result["status"] = "PASS"
        logger.info("ENRICHMENT STAGE completed successfully.")
        stage_results["enrichment"] = stage_result
        return stage_results
    except Exception as e:
        if not stage_result["error_details"].get("errors_by_table"):
            stage_result["error_details"]["errors_by_table"] = {
                table_name: [str(e)]
            }
        if (
            staged_result
            and relocation_target is None
            and stage_result["relocation_status"] == "NOT_APPLICABLE"
        ):
            try:
                relocation = relocate_staged_trusted_files(
                    pipeline_config,
                    staged_result,
                    target="quarantine",
                )
                apply_relocation_result(stage_result, relocation)
            except Exception as relocation_exception:
                stage_result["relocation_status"] = "FAILED"
                stage_result["relocation_error"] = str(relocation_exception)
                stage_result["relocation_details"] = {
                    "moved": [],
                    "errors": [str(relocation_exception)],
                }
        err = StageExecutionError("enrichment", str(e), stage_result)
        handle_unexpected_error(err, run_context, stage_results, write_fn)
        raise err from e


def build_run_context() -> Dict[str, Any]:
    execution_id = str(uuid.uuid4())
    batch_ts = datetime.now(timezone.utc).isoformat()
    run_context = {"execution_id": execution_id, "batch_ts": batch_ts}
    return run_context


def build_quality_report_and_send_webhook(run_context: Dict[str, Any], stage_results: Dict[str, Any]) -> None:
    pipeline_config = load_pipeline_config()
    try:
        report = create_data_quality_report(
            config=pipeline_config,
            execution_id=run_context["execution_id"],
            stage_results=stage_results,
            batch_ts=run_context["batch_ts"],
        )
        send_webhook_from_report(report, pipeline_config, "success path")
    except Exception as e:
        logger.error("build_quality_report_and_send_webhook failed: %s", e)
        raise


def handle_unexpected_error(e: StageExecutionError, run_context: Dict[str, Any], stage_results: Dict[str, Any], write_fn: Optional[Callable[..., Any]] = None) -> None:
    pipeline_config = load_pipeline_config()
    stage_results[e.stage] = e.stage_result
    logger.error("%s failed: %s", e.stage, e)
    report = create_failure_quality_report(
        config=pipeline_config,
        execution_id=run_context["execution_id"],
        failure_phase=e.stage,
        failure_message=str(e),
        stage_results=stage_results,
        batch_ts=run_context["batch_ts"],
        write_fn=write_fn,
    )
    send_webhook_from_report(report, pipeline_config, "failure path")
