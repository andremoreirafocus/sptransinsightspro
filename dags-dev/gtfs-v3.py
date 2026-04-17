from datetime import datetime, timezone
import logging
from logging.handlers import RotatingFileHandler
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
from infra.object_storage import read_file_from_object_storage_to_bytesio
from quality.validate_expectations import validate_expectations
from infra.notifications import send_webhook
from pipeline_configurator.config import get_config
import pandas as pd

LOG_FILENAME = "gtfs.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILENAME, maxBytes=5 * 1024 * 1024, backupCount=5),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)

PIPELINE_NAME = "gtfs"


class StageExecutionError(ValueError):
    def __init__(self, stage: str, message: str, stage_result: dict):
        super().__init__(message)
        self.stage = stage
        self.stage_result = stage_result


def _load_pipeline_config():
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


def _send_webhook_from_report(report: dict, pipeline_config: dict, path: str):
    summary = report.get("summary", {})
    webhook_url = pipeline_config["general"]["notifications"]["webhook_url"]
    if webhook_url.strip().lower() in {"disabled", "none", "null"}:
        logger.info("Webhook notification disabled (%s)", path)
        return
    try:
        send_webhook(summary, webhook_url)
        logger.info("Webhook notification sent (%s)", path)
    except Exception as e:
        logger.error("Webhook notification failed: %s", e)


def extract_load_files(pipeline_config=None):
    stage_result = {
        "status": "FAIL",
        "validated_items_count": 0,
        "error_details": {},
        "relocation_status": "NOT_APPLICABLE",
        "relocation_error": None,
    }
    cfg = pipeline_config or _load_pipeline_config()
    try:
        logger.info("=== EXTRACT & LOAD STAGE: extract_gtfs_files ===")
        files_list = extract_gtfs_files(cfg)
        if not files_list:
            raise ValueError("No GTFS files extracted.")

        logger.info("=== EXTRACT & LOAD STAGE: validate_raw_gtfs_files ===")
        validation_result = validate_raw_gtfs_files(cfg, files_list)
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
                save_files_to_raw_storage(cfg, files_list, failed=True)
                stage_result["relocation_status"] = "SUCCESS"
                logger.info("All extracted files saved to quarantine.")
            except Exception as e:
                stage_result["relocation_status"] = "FAILED"
                stage_result["relocation_error"] = str(e)
                raise ValueError(f"Validation failed and quarantine save failed: {e}")
            raise ValueError(f"Raw GTFS validation failed: {consolidated_reasons}")

        save_files_to_raw_storage(cfg, files_list)
        stage_result["status"] = "PASS"
        logger.info("EXTRACT & LOAD STAGE completed successfully.")
        return stage_result
    except Exception as e:
        if not stage_result.get("error_details"):
            stage_result["error_details"] = {"errors": [str(e)]}
        raise StageExecutionError("extract_load_files", str(e), stage_result)


def transform(pipeline_config=None):
    cfg = pipeline_config or _load_pipeline_config()
    stage_result = {
        "status": "FAIL",
        "validated_items_count": 0,
        "error_details": {},
        "relocation_status": "NOT_APPLICABLE",
        "relocation_error": None,
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
            table_results.append(transform_and_validate_table(cfg, table_name))
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
                cfg,
                staged_results,
                target="quarantine",
            )
            stage_result["relocation_status"] = relocation["status"]
            if relocation.get("errors"):
                stage_result["relocation_error"] = str(relocation["errors"])
                stage_result["error_details"]["relocation_errors"] = relocation["errors"]
            consolidated_reasons = "; ".join(
                [f"{table}:{reasons}" for table, reasons in errors_by_table.items()]
            )
            error_message = (
                "Validation failures detected: "
                f"{consolidated_reasons}"
            )
            if stage_result["relocation_error"]:
                error_message = (
                    f"{error_message}; "
                    f"relocation_error:{stage_result['relocation_error']}"
                )
            raise ValueError(error_message)

        relocation = relocate_staged_trusted_files(
            cfg,
            staged_results,
            target="final",
        )
        stage_result["relocation_status"] = relocation["status"]
        if relocation.get("errors"):
            stage_result["relocation_error"] = str(relocation["errors"])
            stage_result["error_details"]["relocation_errors"] = relocation["errors"]
            raise ValueError(
                "staging_to_final_relocation_error:"
                f"{stage_result['relocation_error']}"
            )

        stage_result["status"] = "PASS"
        logger.info("TRANSFORMATION STAGE completed successfully.")
        return stage_result
    except Exception as e:
        if not stage_result.get("error_details"):
            stage_result["error_details"] = {"errors": [str(e)]}
        raise StageExecutionError("transformation", str(e), stage_result)


def create_trip_details(pipeline_config=None):
    cfg = pipeline_config or _load_pipeline_config()
    table_name = cfg["general"]["tables"]["trip_details_table_name"]
    staged_result = []
    relocation_target = None
    stage_result = {
        "status": "FAIL",
        "validated_items_count": 1,
        "error_details": {"errors_by_table": {}},
        "relocation_status": "NOT_APPLICABLE",
        "relocation_error": None,
    }
    try:
        logger.info("=== ENRICHMENT STAGE: create_trip_details ===")
        creation_result = create_trip_details_table_and_fill_missing_data(cfg)
        staging_object_name = creation_result["staging_object_name"]
        staged_result = [
            {
                "table_name": table_name,
                "staging_object_name": staging_object_name,
                "staged_written": True,
            }
        ]

        suite = cfg.get("data_expectations_trip_details")
        if isinstance(suite, dict) and len(suite.get("expectations", [])) > 0:
            logger.info(
                "ENRICHMENT STAGE - Running expectations validation for table '%s'",
                table_name,
            )
            storage = cfg["general"]["storage"]
            trusted_bucket = storage["trusted_bucket"]
            connection_data = {
                **cfg["connections"]["object_storage"],
                "secure": False,
            }
            parquet_buffer = read_file_from_object_storage_to_bytesio(
                connection_data,
                bucket_name=trusted_bucket,
                object_name=staging_object_name,
            )
            trip_details_df = pd.read_parquet(parquet_buffer)
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
                    cfg,
                    staged_result,
                    target="quarantine",
                )
                stage_result["relocation_status"] = relocation["status"]
                if relocation.get("errors"):
                    stage_result["relocation_error"] = str(relocation["errors"])
                    stage_result["error_details"]["relocation_errors"] = relocation[
                        "errors"
                    ]
                raise ValueError(f"Validation failures detected: {table_name}:{summary}")
        else:
            logger.info(
                "Validation not required and skipped for table %s",
                table_name,
            )

        relocation_target = "final"
        relocation = relocate_staged_trusted_files(
            cfg,
            staged_result,
            target="final",
        )
        stage_result["relocation_status"] = relocation["status"]
        if relocation.get("errors"):
            stage_result["relocation_error"] = str(relocation["errors"])
            stage_result["error_details"]["relocation_errors"] = relocation["errors"]
            raise ValueError(
                "staging_to_final_relocation_error:"
                f"{stage_result['relocation_error']}"
            )
        stage_result["status"] = "PASS"
        logger.info("ENRICHMENT STAGE completed successfully.")
        return stage_result
    except Exception as e:
        if not stage_result["error_details"].get("errors_by_table"):
            stage_result["error_details"]["errors_by_table"] = {table_name: [str(e)]}
        if (
            staged_result
            and relocation_target is None
            and stage_result["relocation_status"] == "NOT_APPLICABLE"
        ):
            try:
                relocation = relocate_staged_trusted_files(
                    cfg,
                    staged_result,
                    target="quarantine",
                )
                stage_result["relocation_status"] = relocation["status"]
                if relocation.get("errors"):
                    stage_result["relocation_error"] = str(relocation["errors"])
                    stage_result["error_details"]["relocation_errors"] = relocation[
                        "errors"
                    ]
            except Exception as relocation_exception:
                stage_result["relocation_status"] = "FAILED"
                stage_result["relocation_error"] = str(relocation_exception)
        raise StageExecutionError("enrichment", str(e), stage_result)


def main():
    pipeline_config = _load_pipeline_config()
    execution_id = str(uuid.uuid4())
    batch_ts = datetime.now(timezone.utc).isoformat()
    stage_results = {}
    try:
        stage_results["extract_load_files"] = extract_load_files(pipeline_config)
        stage_results["transformation"] = transform(pipeline_config)
        stage_results["enrichment"] = create_trip_details(pipeline_config)
        report = create_data_quality_report(
            config=pipeline_config,
            execution_id=execution_id,
            stage_results=stage_results,
            batch_ts=batch_ts,
        )
        _send_webhook_from_report(report, pipeline_config, "success path")
    except StageExecutionError as e:
        stage_results[e.stage] = e.stage_result
        logger.error("%s failed: %s", e.stage, e)
        report = create_failure_quality_report(
            config=pipeline_config,
            execution_id=execution_id,
            failure_phase=e.stage,
            failure_message=str(e),
            stage_results=stage_results,
            batch_ts=batch_ts,
        )
        _send_webhook_from_report(report, pipeline_config, "failure path")
        raise


if __name__ == "__main__":
    main()
