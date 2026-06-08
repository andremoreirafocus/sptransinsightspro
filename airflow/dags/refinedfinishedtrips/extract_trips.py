from datetime import datetime, timezone
from typing import Any, Dict
import uuid
from zoneinfo import ZoneInfo

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
    handle_failure_event,
    handle_phase_metrics_event,
)
from quality.execution_phase_metrics import ExecutionPhaseMetricsTracker
from observability.structured_event_logger import get_structured_logger, set_execution_context
from refinedfinishedtrips.domain.logger import RefinedFinishedTripsLogger


def _build_column_lineage() -> Dict[str, Any]:
    column_lineage = get_finished_trips_lineage()
    actual_output_columns = get_finished_trips_output_columns()
    return validate_finished_trips_lineage(column_lineage, actual_output_columns)


def extract_trips_for_all_Lines_and_vehicles(
    pipeline_name: str,
    deps: RefinedFinishedTripsOrchestrationDependencies | None = None,
    *,
    correlation_id: str,
    logic_date_str: str,
) -> None:
    def create_execution_aborted_log_record(message: str, phase: str) -> None:
        structured_logger.error(
            event="execution_aborted",
            message=f"Pipeline aborted: {message}",
            status="FAILED",
            metadata={"phase": phase},
        )

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
    run_ts_utc = datetime.now(timezone.utc)
    run_ts_localtime = run_ts_utc.astimezone(ZoneInfo("America/Sao_Paulo"))
    structured_logger = RefinedFinishedTripsLogger(
        get_structured_logger(service="refinedfinishedtrips", component="orchestrator", logger_name=__name__)
    )
    effective_correlation_id = correlation_id
    logic_date: datetime = datetime.fromisoformat(logic_date_str)
    state = PipelineTaskRunState(
        execution_id=execution_id,
        correlation_id=effective_correlation_id,
        run_ts=run_ts_localtime,
    )
    set_execution_context(state.execution_id, state.correlation_id)
    tracker = ExecutionPhaseMetricsTracker(
        pipeline=str(pipeline_name),
        execution_id=execution_id,
        logical_date_utc=run_ts_utc.isoformat(),
        phase_order=phase_order,
    )
    structured_logger.info(
        event="execution_started",
        message="Starting execution",
        status="STARTED",
    )
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
        state.pipeline_config = pipeline_config
        structured_logger.info(
            event="config_load_succeeded",
            message="Configuration load succeeded",
            status="SUCCEEDED",
        )
    except Exception as e:
        tracker.finish("config_load", "failed")
        error_msg = "Configuration load and validation failed"
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record("Config load failed.", phase="config_load")
        raise ValueError(error_msg) from e
    positions_result = None
    trips_result = None
    persistence_result = None
    extraction_metrics: Dict[str, Any] = {}
    column_lineage = None
    structured_logger.info(
        event="positions_load_started",
        message="Starting positions load",
        status="STARTED",
    )
    tracker.begin("positions_load")
    try:
        df_recent_positions = deps.get_recent_positions(pipeline_config)
        tracker.finish("positions_load", "success")
    except Exception as exc:
        tracker.finish("positions_load", "failed")
        handle_failure_event(state, deps, structured_logger, "positions_load", str(exc))
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record("Positions load failed.", phase="positions_load")
        raise

    structured_logger.info(
        event="positions_quality_started",
        message="Validating positions quality",
        status="STARTED",
        metadata={"record_count": len(df_recent_positions)},
    )
    tracker.begin("positions_quality")
    try:
        positions_result = deps.validate_positions_quality(pipeline_config, df_recent_positions)
        state.positions_result = positions_result
        if positions_result["status"] == "FAIL":
            failed_notes = "; ".join(
                c.get("reason", f"{c['check']} check failed")
                for c in positions_result.get("checks", [])
                if c["status"] == "FAIL"
            )
            raise ValueError(f"Positions quality check FAILED: {failed_notes}")
        tracker.finish("positions_quality", "success")
    except Exception as exc:
        tracker.finish("positions_quality", "failed")
        handle_failure_event(state, deps, structured_logger, "positions_quality", str(exc))
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record("Positions quality check failed.", phase="positions_quality")
        raise

    tracker.begin("trip_extraction")
    try:
        all_finished_trips, extraction_metrics = deps.get_all_finished_trips(pipeline_config, df_recent_positions)
        column_lineage = _build_column_lineage()
        state.column_lineage = column_lineage
        trips_result = deps.validate_trips_quality(
            pipeline_config, df_recent_positions, all_finished_trips, extraction_metrics
        )
        state.trips_result = trips_result
        tracker.finish("trip_extraction", "success")
    except Exception as exc:
        tracker.finish("trip_extraction", "failed")
        failure_message = str(exc)
        handle_failure_event(state, deps, structured_logger, "trip_extraction", failure_message)
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record(failure_message, phase="trip_extraction")
        raise
    tracker.begin("persistence")
    try:
        save_result = deps.save_finished_trips_to_db(pipeline_config, all_finished_trips, logic_date=logic_date)
        persistence_result = {"status": "PASS", **save_result}
        state.persistence_result = persistence_result
        tracker.finish("persistence", "success")
    except Exception as exc:
        tracker.finish("persistence", "failed")
        failure_message = str(exc)
        handle_failure_event(state, deps, structured_logger, "persistence", failure_message)
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record(failure_message, phase="persistence")
        raise
    tracker.begin("quality_report")
    try:
        report = deps.create_final_quality_report(
            pipeline_config,
            execution_id,
            run_ts_localtime,
            positions_result,
            trips_result,
            persistence_result,
            column_lineage=column_lineage,
        )
        tracker.finish("quality_report", "success")
        structured_logger.info(
            event="quality_report_metrics",
            message="Quality report metrics",
            status="SUCCEEDED",
            metadata=report["summary"],
        )
        handle_phase_metrics_event(state, tracker, structured_logger, "success")
    except Exception:
        tracker.finish("quality_report", "failed")
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record("Quality report failed.", phase="quality_report")
        raise
    structured_logger.info(
        event="execution_finished",
        message="Pipeline run complete",
        status="SUCCEEDED",
        metadata={"report_status": report["summary"]["status"]},
    )
