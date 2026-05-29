from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Any, Dict, Optional
import uuid

from gtfs.config.gtfs_config_schema import GeneralConfig
from gtfs.config.observability import THIRD_PARTY_LOGGER_NAMESPACES
from gtfs.domain.logger import GtfsLogger
from gtfs.lineage.trip_details_lineage import (
    get_trip_details_lineage,
    validate_trip_details_lineage,
)
from gtfs.orchestration_dependencies import (
    GtfsOrchestrationDependencies,
    get_gtfs_orchestration_dependencies,
)
from observability.structured_event_logger import get_structured_logger, set_execution_context
from observability.third_party_log_bridge import configure_third_party_log_bridge
from quality.execution_phase_metrics import ExecutionPhaseMetricsTracker
import pandas as pd

PIPELINE_NAME = "gtfs"
PHASE_ORDER = [
    "extract_load_files",
    "transformation",
    "enrichment",
    "quality_report",
]

_structured_logger = GtfsLogger(
    get_structured_logger(
        service=PIPELINE_NAME,
        component="orchestrator",
        logger_name=__name__,
    )
)


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


def _build_consolidated_metrics(
    stage_results: Dict[str, Any],
    current_phase: str,
    tracker: ExecutionPhaseMetricsTracker,
    overall_status: str,
) -> Dict[str, Any]:
    full = {phase: {"duration_seconds": 0.0, "status": "skipped"} for phase in PHASE_ORDER}
    full.update(stage_results.get("phase_metrics", {}))
    full[current_phase] = tracker.to_log_payload(overall_status)["phase_metrics"][current_phase]
    return full


def apply_relocation_result(stage_result: dict, relocation: dict) -> None:
    stage_result["relocation_status"] = relocation.get("status", "FAILED")
    stage_result["relocation_details"] = {
        "moved": relocation.get("moved", []),
        "errors": relocation.get("errors", []),
    }
    if relocation.get("errors"):
        stage_result["relocation_error"] = str(relocation["errors"])
        stage_result["error_details"]["relocation_errors"] = relocation["errors"]


def extract_load_files(run_context: Dict[str, Any], stage_results: Dict[str, Any], deps: Optional[GtfsOrchestrationDependencies] = None) -> Dict[str, Any]:
    if deps is None:
        deps = get_gtfs_orchestration_dependencies()
    set_execution_context(run_context["execution_id"], correlation_id=run_context["batch_ts"])
    configure_third_party_log_bridge(
        structured_logger=_structured_logger._logger,
        execution_id=run_context["execution_id"],
        correlation_id=run_context["batch_ts"],
        namespaces=THIRD_PARTY_LOGGER_NAMESPACES,
    )
    _structured_logger.info(event="execution_started", message="Starting execution", status="STARTED")
    tracker = ExecutionPhaseMetricsTracker(
        pipeline=PIPELINE_NAME,
        execution_id=run_context["execution_id"],
        logical_date_utc=run_context["batch_ts"],
        phase_order=PHASE_ORDER,
    )
    tracker.begin("extract_load_files")
    pipeline_config = deps.get_config(PIPELINE_NAME, None, GeneralConfig, "gtfs_conn", "minio_conn", None)
    stage_result: Dict[str, Any] = {
        "status": "FAIL",
        "validated_items_count": 0,
        "error_details": {},
        "relocation_status": "NOT_APPLICABLE",
        "relocation_error": None,
        "relocation_details": RelocationDetails().to_dict(),
    }
    try:
        files_list = deps.extract_gtfs_files(pipeline_config)
        if not files_list:
            raise ValueError("No GTFS files extracted.")

        validation_result = deps.validate_raw_gtfs_files(pipeline_config, files_list)
        errors_by_file = validation_result.get("errors_by_file", {})
        stage_result["validated_items_count"] = validation_result.get(
            "validated_files_count", 0
        )
        stage_result["error_details"] = {"errors_by_file": errors_by_file}

        if not validation_result.get("is_valid", False):
            consolidated_reasons = "; ".join(
                [f"{file}:{reasons}" for file, reasons in errors_by_file.items()]
            )
            try:
                deps.save_files_to_raw_storage(pipeline_config, files_list, failed=True)
                stage_result["relocation_status"] = "SUCCESS"
                stage_result["relocation_details"] = {
                    "moved": [
                        {"scope": "all_extracted_files", "target": "quarantine"}
                    ],
                    "errors": [],
                }
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

        deps.save_files_to_raw_storage(pipeline_config, files_list)
        stage_result["status"] = "PASS"
        stage_results["extract_load_files"] = stage_result
        tracker.finish("extract_load_files", "success")
        stage_results.setdefault("phase_metrics", {})["extract_load_files"] = (
            tracker.to_log_payload("success")["phase_metrics"]["extract_load_files"]
        )
        return stage_results
    except Exception as e:
        tracker.finish("extract_load_files", "failed")
        if not stage_result.get("error_details"):
            stage_result["error_details"] = {"errors": [str(e)]}
        err = StageExecutionError("extract_load_files", str(e), stage_result)
        handle_unexpected_error(err, run_context, stage_results, deps)
        stage_results.setdefault("phase_metrics", {})["extract_load_files"] = (
            tracker.to_log_payload("failed")["phase_metrics"]["extract_load_files"]
        )
        _structured_logger.error(
            event="execution_phase_metrics",
            message="Execution phase metrics",
            status="FAILED",
            metadata={
                "pipeline": PIPELINE_NAME,
                "execution_id": run_context["execution_id"],
                "logical_date_utc": run_context["batch_ts"],
                "overall_status": "failed",
                "phase_metrics": _build_consolidated_metrics(stage_results, "extract_load_files", tracker, "failed"),
            },
        )
        _structured_logger.error(
            event="execution_aborted",
            message="Pipeline aborted: extract load files failed",
            status="FAILED",
            metadata={"phase": "extract_load_files"},
        )
        raise err from e


def transform(run_context: Dict[str, Any], stage_results: Dict[str, Any], deps: Optional[GtfsOrchestrationDependencies] = None) -> Dict[str, Any]:
    if deps is None:
        deps = get_gtfs_orchestration_dependencies()
    set_execution_context(run_context["execution_id"], correlation_id=run_context["batch_ts"])
    configure_third_party_log_bridge(
        structured_logger=_structured_logger._logger,
        execution_id=run_context["execution_id"],
        correlation_id=run_context["batch_ts"],
        namespaces=THIRD_PARTY_LOGGER_NAMESPACES,
    )
    tracker = ExecutionPhaseMetricsTracker(
        pipeline=PIPELINE_NAME,
        execution_id=run_context["execution_id"],
        logical_date_utc=run_context["batch_ts"],
        phase_order=PHASE_ORDER,
    )
    tracker.begin("transformation")
    pipeline_config = deps.get_config(PIPELINE_NAME, None, GeneralConfig, "gtfs_conn", "minio_conn", None)
    stage_result: Dict[str, Any] = {
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
        for table_name in table_names:
            table_results.append(
                deps.transform_and_validate_table(pipeline_config, table_name)
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
            relocation = deps.relocate_staged_trusted_files(
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
        relocation = deps.relocate_staged_trusted_files(
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
        stage_results["transformation"] = stage_result
        tracker.finish("transformation", "success")
        stage_results.setdefault("phase_metrics", {})["transformation"] = (
            tracker.to_log_payload("success")["phase_metrics"]["transformation"]
        )
        return stage_results
    except Exception as e:
        tracker.finish("transformation", "failed")
        if not stage_result.get("error_details"):
            stage_result["error_details"] = {"errors": [str(e)]}
        err = StageExecutionError("transformation", str(e), stage_result)
        handle_unexpected_error(err, run_context, stage_results, deps)
        stage_results.setdefault("phase_metrics", {})["transformation"] = (
            tracker.to_log_payload("failed")["phase_metrics"]["transformation"]
        )
        _structured_logger.error(
            event="execution_phase_metrics",
            message="Execution phase metrics",
            status="FAILED",
            metadata={
                "pipeline": PIPELINE_NAME,
                "execution_id": run_context["execution_id"],
                "logical_date_utc": run_context["batch_ts"],
                "overall_status": "failed",
                "phase_metrics": _build_consolidated_metrics(stage_results, "transformation", tracker, "failed"),
            },
        )
        _structured_logger.error(
            event="execution_aborted",
            message="Pipeline aborted: transformation failed",
            status="FAILED",
            metadata={"phase": "transformation"},
        )
        raise err from e


def create_trip_details(run_context: Dict[str, Any], stage_results: Dict[str, Any], deps: Optional[GtfsOrchestrationDependencies] = None) -> Dict[str, Any]:
    if deps is None:
        deps = get_gtfs_orchestration_dependencies()
    set_execution_context(run_context["execution_id"], correlation_id=run_context["batch_ts"])
    configure_third_party_log_bridge(
        structured_logger=_structured_logger._logger,
        execution_id=run_context["execution_id"],
        correlation_id=run_context["batch_ts"],
        namespaces=THIRD_PARTY_LOGGER_NAMESPACES,
    )
    tracker = ExecutionPhaseMetricsTracker(
        pipeline=PIPELINE_NAME,
        execution_id=run_context["execution_id"],
        logical_date_utc=run_context["batch_ts"],
        phase_order=PHASE_ORDER,
    )
    tracker.begin("enrichment")
    pipeline_config = deps.get_config(PIPELINE_NAME, None, GeneralConfig, "gtfs_conn", "minio_conn", None)
    table_name = pipeline_config["general"]["tables"]["trip_details_table_name"]
    staged_result = []
    relocation_target = None
    stage_result: Dict[str, Any] = {
        "status": "FAIL",
        "validated_items_count": 1,
        "error_details": {"errors_by_table": {}},
        "artifacts": {"column_lineage": get_trip_details_lineage()},
        "relocation_status": "NOT_APPLICABLE",
        "relocation_error": None,
        "relocation_details": RelocationDetails().to_dict(),
    }
    try:
        creation_result = deps.create_trip_details_table_and_fill_missing_data(
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
            storage = pipeline_config["general"]["storage"]
            trusted_bucket = storage["trusted_bucket"]
            connection_data = {
                **pipeline_config["connections"]["object_storage"],
                "secure": False,
            }
            parquet_buffer = deps.read_file_from_object_storage_to_bytesio(
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
            expectations_result = deps.validate_expectations(trip_details_df, suite)
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
                relocation = deps.relocate_staged_trusted_files(
                    pipeline_config,
                    staged_result,
                    target="quarantine",
                )
                apply_relocation_result(stage_result, relocation)
                raise ValueError(
                    f"Validation failures detected: {table_name}:{summary}"
                )

        relocation_target = "final"
        relocation = deps.relocate_staged_trusted_files(
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
        stage_results["enrichment"] = stage_result
        tracker.finish("enrichment", "success")
        stage_results.setdefault("phase_metrics", {})["enrichment"] = (
            tracker.to_log_payload("success")["phase_metrics"]["enrichment"]
        )
        return stage_results
    except Exception as e:
        tracker.finish("enrichment", "failed")
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
                relocation = deps.relocate_staged_trusted_files(
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
        handle_unexpected_error(err, run_context, stage_results, deps)
        stage_results.setdefault("phase_metrics", {})["enrichment"] = (
            tracker.to_log_payload("failed")["phase_metrics"]["enrichment"]
        )
        _structured_logger.error(
            event="execution_phase_metrics",
            message="Execution phase metrics",
            status="FAILED",
            metadata={
                "pipeline": PIPELINE_NAME,
                "execution_id": run_context["execution_id"],
                "logical_date_utc": run_context["batch_ts"],
                "overall_status": "failed",
                "phase_metrics": _build_consolidated_metrics(stage_results, "enrichment", tracker, "failed"),
            },
        )
        _structured_logger.error(
            event="execution_aborted",
            message="Pipeline aborted: enrichment failed",
            status="FAILED",
            metadata={"phase": "enrichment"},
        )
        raise err from e


def build_run_context() -> Dict[str, Any]:
    execution_id = str(uuid.uuid4())
    batch_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat()
    return {"execution_id": execution_id, "batch_ts": batch_ts}


def build_quality_report(run_context: Dict[str, Any], stage_results: Dict[str, Any], deps: Optional[GtfsOrchestrationDependencies] = None) -> None:
    if deps is None:
        deps = get_gtfs_orchestration_dependencies()
    set_execution_context(run_context["execution_id"], correlation_id=run_context["batch_ts"])
    configure_third_party_log_bridge(
        structured_logger=_structured_logger._logger,
        execution_id=run_context["execution_id"],
        correlation_id=run_context["batch_ts"],
        namespaces=THIRD_PARTY_LOGGER_NAMESPACES,
    )
    tracker = ExecutionPhaseMetricsTracker(
        pipeline=PIPELINE_NAME,
        execution_id=run_context["execution_id"],
        logical_date_utc=run_context["batch_ts"],
        phase_order=PHASE_ORDER,
    )
    tracker.begin("quality_report")
    pipeline_config = deps.get_config(PIPELINE_NAME, None, GeneralConfig, "gtfs_conn", "minio_conn", None)
    try:
        deps.create_data_quality_report(
            config=pipeline_config,
            execution_id=run_context["execution_id"],
            stage_results=stage_results,
            batch_ts=run_context["batch_ts"],
        )
        tracker.finish("quality_report", "success")
        stage_results.setdefault("phase_metrics", {})["quality_report"] = (
            tracker.to_log_payload("success")["phase_metrics"]["quality_report"]
        )
        _structured_logger.info(
            event="execution_phase_metrics",
            message="Execution phase metrics",
            status="SUCCEEDED",
            metadata={
                "pipeline": PIPELINE_NAME,
                "execution_id": run_context["execution_id"],
                "logical_date_utc": run_context["batch_ts"],
                "overall_status": "success",
                "phase_metrics": _build_consolidated_metrics(stage_results, "quality_report", tracker, "success"),
            },
        )
        _structured_logger.info(event="execution_finished", message="Pipeline run complete", status="SUCCEEDED")
    except Exception:
        tracker.finish("quality_report", "failed")
        stage_results.setdefault("phase_metrics", {})["quality_report"] = (
            tracker.to_log_payload("failed")["phase_metrics"]["quality_report"]
        )
        _structured_logger.error(
            event="execution_phase_metrics",
            message="Execution phase metrics",
            status="FAILED",
            metadata={
                "pipeline": PIPELINE_NAME,
                "execution_id": run_context["execution_id"],
                "logical_date_utc": run_context["batch_ts"],
                "overall_status": "failed",
                "phase_metrics": _build_consolidated_metrics(stage_results, "quality_report", tracker, "failed"),
            },
        )
        _structured_logger.error(
            event="execution_aborted",
            message="Pipeline aborted: quality report failed",
            status="FAILED",
            metadata={"phase": "quality_report"},
        )
        raise


def handle_unexpected_error(e: StageExecutionError, run_context: Dict[str, Any], stage_results: Dict[str, Any], deps: GtfsOrchestrationDependencies) -> None:
    pipeline_config = deps.get_config(PIPELINE_NAME, None, GeneralConfig, "gtfs_conn", "minio_conn", None)
    stage_results[e.stage] = e.stage_result
    _structured_logger.error(
        event="execution_aborted",
        message=f"Pipeline aborted: {e.stage} failed",
        status="FAILED",
        metadata={"phase": e.stage},
    )
    deps.create_failure_quality_report(
        config=pipeline_config,
        execution_id=run_context["execution_id"],
        failure_phase=e.stage,
        failure_message=str(e),
        stage_results=stage_results,
        batch_ts=run_context["batch_ts"],
        write_fn=deps.write_fn,
    )
