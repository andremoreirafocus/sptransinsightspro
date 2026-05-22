from datetime import datetime, timezone
from typing import Any, Callable, Dict
import uuid

from refinedfinishedtrips.config.refinedfinishedtrips_config_schema import (
    GeneralConfig,
)
from refinedfinishedtrips.lineage import (
    get_finished_trips_lineage,
    get_finished_trips_output_columns,
    validate_finished_trips_lineage,
)
from refinedfinishedtrips.orchestration_dependencies import (
    RefinedFinishedTripsOrchestrationDependencies,
    get_refinedfinishedtrips_orchestration_dependencies,
)
from refinedfinishedtrips.orchestration_event_handlers import (
    PipelineTaskRunState,
    handle_phase_metrics_event,
)
from quality.execution_phase_metrics import ExecutionPhaseMetricsTracker
from observability.structured_event_logger import get_structured_logger
from refinedfinishedtrips.domain.logger import RefinedFinishedTripsLogger
import logging

logger = logging.getLogger(__name__)



def _build_column_lineage() -> Dict[str, Any]:
    column_lineage = get_finished_trips_lineage()
    actual_output_columns = get_finished_trips_output_columns()
    return validate_finished_trips_lineage(column_lineage, actual_output_columns)


def _handle_positions_result(
    positions_result: Dict[str, Any],
    pipeline_config: Dict[str, Any],
    execution_id: str,
    run_ts: datetime,
    create_report_fn: Callable[..., Any],
    create_failure_report_fn: Callable[..., Any],
) -> None:
    def checks_message(status):
        return "; ".join(
            c.get("note", f"{c['check']} check {status.lower()}")
            for c in positions_result["checks"]
            if c["status"] == status
        )
    if positions_result["status"] == "FAIL":
        failure_message = checks_message("FAIL")
        logger.error(f"Positions quality FAIL: {failure_message}")
        create_failure_report_fn(
            pipeline_config,
            execution_id,
            run_ts,
            "positions",
            failure_message,
            positions_result,
        )
        raise ValueError(f"Positions quality check FAILED: {failure_message}")
    if positions_result["status"] == "WARN":
        logger.warning(f"Positions quality WARN: {checks_message('WARN')}")
        create_report_fn(pipeline_config, execution_id, run_ts, positions_result)


def _handle_trips_result(trips_result: Dict[str, Any]) -> None:
    if trips_result["status"] == "WARN":
        warn_notes = "; ".join(
            c.get("note", f"{c['check']} check warn")
            for c in trips_result["checks"]
            if c["status"] == "WARN"
        )
        logger.warning(f"Trip extraction quality WARN: {warn_notes}")


def _handle_persistence_result(persistence_result: Dict[str, Any]) -> None:
    if persistence_result["status"] == "WARN":
        logger.warning(f"Persistence quality WARN: {persistence_result.get('note', 'all trips were duplicates')}")


def extract_trips_for_all_Lines_and_vehicles(
    pipeline_name: str,
    deps: RefinedFinishedTripsOrchestrationDependencies | None = None,
) -> None:
    if deps is None:
        deps = get_refinedfinishedtrips_orchestration_dependencies()
    phase_order = [
        "config_load",
        "positions_load",
        "positions_quality",
        "trip_extraction",
        "persistence",
        "quality_report",
    ]
    execution_id = str(uuid.uuid4())
    run_ts = datetime.now(timezone.utc)
    tracker = ExecutionPhaseMetricsTracker(
        pipeline=str(pipeline_name),
        execution_id=execution_id,
        logical_date_utc=run_ts.isoformat(),
        phase_order=phase_order,
    )
    logger.info("Starting execution")
    structured_logger = RefinedFinishedTripsLogger(
        get_structured_logger(service="refinedfinishedtrips", component="orchestrator", logger_name=__name__)
    )  # upgraded in Step 5
    state = PipelineTaskRunState(execution_id=execution_id, correlation_id=execution_id, run_ts=run_ts)
    tracker.begin("config_load")
    try:
        pipeline_config = deps.get_config(
            pipeline_name,
            None,
            GeneralConfig,
            None,
            "minio_conn",
            "postgres_conn",
        )
        tracker.finish("config_load", "success")
        logger.info("Configuration load succeeded")
    except Exception as e:
        tracker.finish("config_load", "failed")
        error_msg = "Configuration load and validation failed"
        logger.error(error_msg)
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        raise ValueError(error_msg) from e
    positions_result = None
    trips_result = None
    persistence_result = None
    extraction_metrics: Dict[str, Any] = {}
    column_lineage = None
    logger.info(f"Starting pipeline run. execution_id={execution_id}")
    tracker.begin("positions_load")
    try:
        df_recent_positions = deps.get_recent_positions(pipeline_config)
        tracker.finish("positions_load", "success")
    except Exception:
        tracker.finish("positions_load", "failed")
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        raise

    logger.info(f"Validating quality of {len(df_recent_positions)} position records.")
    tracker.begin("positions_quality")
    try:
        positions_result = deps.validate_positions_quality(pipeline_config, df_recent_positions)
        _handle_positions_result(
            positions_result,
            pipeline_config,
            execution_id,
            run_ts,
            deps.create_quality_report,
            deps.create_failure_quality_report,
        )
        tracker.finish("positions_quality", "success")
    except Exception:
        tracker.finish("positions_quality", "failed")
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        raise

    tracker.begin("trip_extraction")
    try:
        all_finished_trips, extraction_metrics = deps.get_all_finished_trips(pipeline_config, df_recent_positions)
        column_lineage = _build_column_lineage()
        trips_result = deps.validate_trips_quality(
            pipeline_config, df_recent_positions, all_finished_trips, extraction_metrics
        )
        _handle_trips_result(trips_result)
        tracker.finish("trip_extraction", "success")
    except Exception as exc:
        tracker.finish("trip_extraction", "failed")
        failure_message = str(exc)
        logger.error(f"Trip extraction failed: {failure_message}")
        deps.create_failure_quality_report(
            pipeline_config,
            execution_id,
            run_ts,
            "trip_extraction",
            failure_message,
            positions_result,
            trips_result=trips_result,
            column_lineage=column_lineage,
        )
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        raise
    tracker.begin("persistence")
    try:
        save_result = deps.save_finished_trips_to_db(pipeline_config, all_finished_trips)
        persistence_result = deps.validate_persistence_quality(save_result)
        _handle_persistence_result(persistence_result)
        tracker.finish("persistence", "success")
    except Exception as exc:
        tracker.finish("persistence", "failed")
        failure_message = str(exc)
        logger.error(f"Persistence failed: {failure_message}")
        deps.create_failure_quality_report(
            pipeline_config,
            execution_id,
            run_ts,
            "persistence",
            failure_message,
            positions_result,
            trips_result=trips_result,
            persistence_result=persistence_result,
            column_lineage=column_lineage,
        )
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        raise
    tracker.begin("quality_report")
    try:
        report = deps.create_final_quality_report(
            pipeline_config,
            execution_id,
            run_ts,
            positions_result,
            trips_result,
            persistence_result,
            column_lineage=column_lineage,
        )
        tracker.finish("quality_report", "success")
        handle_phase_metrics_event(state, tracker, structured_logger, "success")
    except Exception:
        tracker.finish("quality_report", "failed")
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        raise
    logger.info(f"Pipeline run complete. execution_id={execution_id}, status={report['summary']['status']}")
