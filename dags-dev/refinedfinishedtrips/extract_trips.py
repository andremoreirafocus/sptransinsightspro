from datetime import datetime, timezone
from typing import Any, Callable, Dict
import uuid

from pipeline_configurator.config import get_config
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
from quality.execution_phase_metrics import ExecutionPhaseMetricsTracker
import logging

logger = logging.getLogger(__name__)


def _load_pipeline_config(pipeline_name: str) -> Dict[str, Any]:
    try:
        pipeline_config = get_config(
            pipeline_name,
            None,
            GeneralConfig,
            None,
            "minio_conn",
            "postgres_conn",
        )
    except Exception as e:
        error_message = (
            "Pipeline configuration validation failed for refinedfinishedtrips: "
            f"pipeline_name='{pipeline_name}'"
        )
        logger.error(error_message)
        raise ValueError(error_message) from e
    return pipeline_config


def _parse_trip_extraction_output(trip_extraction_output: Any) -> tuple[Any, Dict[str, Any]]:
    if (
        isinstance(trip_extraction_output, tuple)
        and len(trip_extraction_output) == 2
        and isinstance(trip_extraction_output[1], dict)
    ):
        return trip_extraction_output
    return trip_extraction_output, {}


def _build_column_lineage() -> Dict[str, Any]:
    column_lineage = get_finished_trips_lineage()
    actual_output_columns = get_finished_trips_output_columns()
    return validate_finished_trips_lineage(column_lineage, actual_output_columns)


def _handle_positions_result(
    positions_result: Dict[str, Any],
    config: Dict[str, Any],
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
            config,
            execution_id,
            run_ts,
            "positions",
            failure_message,
            positions_result,
        )
        raise ValueError(f"Positions quality check FAILED: {failure_message}")
    if positions_result["status"] == "WARN":
        logger.warning(f"Positions quality WARN: {checks_message('WARN')}")
        create_report_fn(config, execution_id, run_ts, positions_result)


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
    pipeline_name_or_config: str | Dict[str, Any],
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
    pipeline_name = (
        pipeline_name_or_config
        if isinstance(pipeline_name_or_config, str)
        else "refinedfinishedtrips"
    )
    execution_id = str(uuid.uuid4())
    run_ts = datetime.now(timezone.utc)
    tracker = ExecutionPhaseMetricsTracker(
        pipeline=str(pipeline_name),
        execution_id=execution_id,
        logical_date_utc=run_ts.isoformat(),
        phase_order=phase_order,
    )
    tracker.begin("config_load")
    try:
        if isinstance(pipeline_name_or_config, dict):
            config = pipeline_name_or_config
        else:
            config = _load_pipeline_config(pipeline_name_or_config)
        tracker.finish("config_load", "success")
    except Exception:
        tracker.finish("config_load", "failed")
        tracker.emit(logger, "failed")
        raise
    positions_result = None
    trips_result = None
    persistence_result = None
    extraction_metrics: Dict[str, Any] = {}
    column_lineage = None
    logger.info(f"Starting pipeline run. execution_id={execution_id}")
    tracker.begin("positions_load")
    try:
        df_recent_positions = deps.get_recent_positions(config)
        tracker.finish("positions_load", "success")
    except Exception:
        tracker.finish("positions_load", "failed")
        tracker.emit(logger, "failed")
        raise

    logger.info(f"Validating quality of {len(df_recent_positions)} position records.")
    tracker.begin("positions_quality")
    try:
        positions_result = deps.validate_positions_quality(config, df_recent_positions)
        _handle_positions_result(
            positions_result,
            config,
            execution_id,
            run_ts,
            deps.create_quality_report,
            deps.create_failure_quality_report,
        )
        tracker.finish("positions_quality", "success")
    except Exception:
        tracker.finish("positions_quality", "failed")
        tracker.emit(logger, "failed")
        raise

    tracker.begin("trip_extraction")
    try:
        trip_extraction_output = deps.get_all_finished_trips(config, df_recent_positions)
        all_finished_trips, extraction_metrics = _parse_trip_extraction_output(
            trip_extraction_output
        )
        column_lineage = _build_column_lineage()
        trips_result = deps.validate_trips_quality(
            config, df_recent_positions, all_finished_trips, extraction_metrics
        )
        _handle_trips_result(trips_result)
        tracker.finish("trip_extraction", "success")
    except Exception as exc:
        tracker.finish("trip_extraction", "failed")
        failure_message = str(exc)
        logger.error(f"Trip extraction failed: {failure_message}")
        deps.create_failure_quality_report(
            config,
            execution_id,
            run_ts,
            "trip_extraction",
            failure_message,
            positions_result,
            trips_result=trips_result,
            column_lineage=column_lineage,
        )
        tracker.emit(logger, "failed")
        raise
    tracker.begin("persistence")
    try:
        save_result = deps.save_finished_trips_to_db(config, all_finished_trips)
        persistence_result = deps.validate_persistence_quality(save_result)
        _handle_persistence_result(persistence_result)
        tracker.finish("persistence", "success")
    except Exception as exc:
        tracker.finish("persistence", "failed")
        failure_message = str(exc)
        logger.error(f"Persistence failed: {failure_message}")
        deps.create_failure_quality_report(
            config,
            execution_id,
            run_ts,
            "persistence",
            failure_message,
            positions_result,
            trips_result=trips_result,
            persistence_result=persistence_result,
            column_lineage=column_lineage,
        )
        tracker.emit(logger, "failed")
        raise
    tracker.begin("quality_report")
    try:
        report = deps.create_final_quality_report(
            config,
            execution_id,
            run_ts,
            positions_result,
            trips_result,
            persistence_result,
            column_lineage=column_lineage,
        )
        tracker.finish("quality_report", "success")
        tracker.emit(logger, "success")
    except Exception:
        tracker.finish("quality_report", "failed")
        tracker.emit(logger, "failed")
        raise
    logger.info(f"Pipeline run complete. execution_id={execution_id}, status={report['summary']['status']}")
