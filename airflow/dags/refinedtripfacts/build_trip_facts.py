import uuid
from datetime import datetime, timezone
from typing import Any, Dict

from observability.structured_event_logger import get_structured_logger, set_execution_context
from quality.execution_phase_metrics import ExecutionPhaseMetricsTracker
from refinedtripfacts.config.refinedtripfacts_config_schema import GeneralConfig
from refinedtripfacts.domain.logger import RefinedTripFactsLogger
from refinedtripfacts.lineage.trip_facts_lineage import (
    get_trip_facts_lineage,
    validate_trip_facts_lineage,
)
from refinedtripfacts.orchestration_dependencies import (
    RefinedTripFactsOrchestrationDependencies,
    get_refinedtripfacts_orchestration_dependencies,
)
from refinedtripfacts.orchestration_event_handlers import (
    PipelineTaskRunState,
    handle_failure_event,
    handle_phase_metrics_event,
)

PIPELINE_NAME = "refinedtripfacts"


def _build_column_lineage(
    deps: RefinedTripFactsOrchestrationDependencies,
    pipeline_config: Dict[str, Any],
) -> Dict[str, Any]:
    lineage = get_trip_facts_lineage()
    actual_columns = deps.get_trip_facts_table_columns(pipeline_config)
    return validate_trip_facts_lineage(lineage, actual_columns)


def build_trip_facts(
    logic_date_str: str,
    deps: RefinedTripFactsOrchestrationDependencies | None = None,
) -> None:
    def create_execution_aborted_log_record(message: str, phase: str) -> None:
        structured_logger.error(
            event="execution_aborted",
            message=f"Pipeline aborted: {message}",
            status="FAILED",
            metadata={"phase": phase},
        )

    if deps is None:
        deps = get_refinedtripfacts_orchestration_dependencies()

    phase_order = [
        "config_load",
        "input_trips_measurement",
        "dim_time_provisioning",
        "trip_facts_creation",
        "trip_facts_verification",
        "data_quality_validation",
        "quality_report",
    ]
    execution_id = str(uuid.uuid4())
    run_ts = datetime.now(timezone.utc)
    correlation_id = logic_date_str
    structured_logger = RefinedTripFactsLogger(
        get_structured_logger(service=PIPELINE_NAME, component="orchestrator", logger_name=__name__)
    )
    state = PipelineTaskRunState(
        execution_id=execution_id,
        correlation_id=correlation_id,
        run_ts=run_ts,
    )
    set_execution_context(state.execution_id, state.correlation_id)
    tracker = ExecutionPhaseMetricsTracker(
        pipeline=PIPELINE_NAME,
        execution_id=execution_id,
        logical_date_utc=run_ts.isoformat(),
        phase_order=phase_order,
    )
    structured_logger.info(event="execution_started", message="Starting execution", status="STARTED")

    logic_date: datetime = datetime.fromisoformat(logic_date_str)

    tracker.begin("config_load")
    structured_logger.info(event="config_load_started", message="Loading configuration", status="STARTED")
    try:
        pipeline_config = deps.get_config(PIPELINE_NAME, None, GeneralConfig, None, "minio_conn", "postgres_conn")
        state.pipeline_config = pipeline_config
    except Exception as e:
        tracker.finish("config_load", "failed")
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record("Config load failed.", phase="config_load")
        raise ValueError("Configuration load and validation failed") from e
    tracker.finish("config_load", "success")
    structured_logger.info(event="config_load_succeeded", message="Configuration loaded", status="SUCCEEDED")

    tracker.begin("input_trips_measurement")
    structured_logger.info(
        event="input_trips_measurement_started",
        message="Measuring input trips",
        status="STARTED",
        metadata={"logic_date": str(logic_date)},
    )
    try:
        measurement = deps.measure_input_trips(logic_date, pipeline_config)
    except Exception as exc:
        tracker.finish("input_trips_measurement", "failed")
        handle_failure_event(state, deps, structured_logger, phase="input_trips_measurement", message=str(exc))
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record(str(exc), phase="input_trips_measurement")
        raise

    number_of_finished_trips_read = measurement["finished_trips_read"]
    state.measurement_result = measurement

    if number_of_finished_trips_read == 0:
        tracker.finish("input_trips_measurement", "failed")
        handle_failure_event(
            state, deps, structured_logger,
            phase="input_trips_measurement",
            message=f"No upstream data for logic_date '{logic_date}'",
        )
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record(
            f"No upstream data for logic_date '{logic_date}'.", phase="input_trips_measurement"
        )
        raise ValueError(f"No finished_trips found upstream for logic_date '{logic_date}'")

    tracker.finish("input_trips_measurement", "success")
    structured_logger.info(
        event="input_trips_measurement_succeeded",
        message="Input trips measured",
        status="SUCCEEDED",
        metadata={"finished_trips_read": number_of_finished_trips_read},
    )

    tracker.begin("dim_time_provisioning")
    structured_logger.info(
        event="dim_time_provisioning_started",
        message="Provisioning dim_time",
        status="STARTED",
        metadata={"logic_date": str(logic_date)},
    )
    try:
        provision = deps.provision_dim_time(logic_date, pipeline_config)
    except Exception as exc:
        tracker.finish("dim_time_provisioning", "failed")
        handle_failure_event(state, deps, structured_logger, phase="dim_time_provisioning", message=str(exc))
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record(str(exc), phase="dim_time_provisioning")
        raise
    state.dim_time_result = provision
    tracker.finish("dim_time_provisioning", "success")
    structured_logger.info(
        event="dim_time_provisioning_succeeded",
        message="dim_time provisioned",
        status="SUCCEEDED",
        metadata={"rows_ensured": provision["rows_ensured"]},
    )

    tracker.begin("trip_facts_creation")
    structured_logger.info(
        event="trip_facts_creation_started",
        message="Creating trip_facts",
        status="STARTED",
        metadata={"logic_date": str(logic_date)},
    )
    try:
        facts_metrics = deps.create_trip_facts(logic_date, pipeline_config)
    except Exception as exc:
        tracker.finish("trip_facts_creation", "failed")
        handle_failure_event(state, deps, structured_logger, phase="trip_facts_creation", message=str(exc))
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record(str(exc), phase="trip_facts_creation")
        raise
    state.creation_result = facts_metrics
    tracker.finish("trip_facts_creation", "success")
    structured_logger.info(
        event="trip_facts_creation_succeeded",
        message="trip_facts created",
        status="SUCCEEDED",
        metadata=facts_metrics,
    )

    tracker.begin("trip_facts_verification")
    structured_logger.info(
        event="trip_facts_verification_started",
        message="Reading back persisted trip_facts",
        status="STARTED",
        metadata={"logic_date": str(logic_date)},
    )
    try:
        persisted_metrics = deps.measure_persisted_facts(logic_date, pipeline_config)
    except Exception as exc:
        tracker.finish("trip_facts_verification", "failed")
        handle_failure_event(state, deps, structured_logger, phase="trip_facts_verification", message=str(exc))
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record(str(exc), phase="trip_facts_verification")
        raise
    state.persisted_metrics = persisted_metrics
    tracker.finish("trip_facts_verification", "success")
    structured_logger.info(
        event="trip_facts_verification_succeeded",
        message="Persisted trip_facts read back",
        status="SUCCEEDED",
        metadata=persisted_metrics,
    )

    tracker.begin("data_quality_validation")
    structured_logger.info(event="data_quality_validation_started", message="Validating data quality", status="STARTED")
    quality = deps.validate_trip_facts_quality(pipeline_config, number_of_finished_trips_read, persisted_metrics)
    state.quality_result = quality
    tracker.finish("data_quality_validation", "success")
    structured_logger.info(
        event="data_quality_validation_succeeded",
        message="Data quality validated",
        status="SUCCEEDED",
        metadata={"status": quality["status"], "loss_rate": quality["loss_rate"]},
    )

    tracker.begin("quality_report")
    structured_logger.info(event="quality_report_started", message="Writing quality report", status="STARTED")
    try:
        try:
            state.column_lineage = _build_column_lineage(deps, pipeline_config)
        except Exception as e:
            state.column_lineage = {
                "drift_detected": None,
                "warning": "lineage validation unavailable",
                "error": str(e),
            }
        report = deps.create_final_quality_report(
            config=pipeline_config,
            execution_id=execution_id,
            run_ts=run_ts,
            measurement_result=state.measurement_result,
            dim_time_result=state.dim_time_result,
            creation_result=state.creation_result,
            persisted_metrics=state.persisted_metrics,
            quality_result=state.quality_result,
            column_lineage=state.column_lineage,
        )
        tracker.finish("quality_report", "success")
        structured_logger.info(event="quality_report_succeeded", message="Quality report written", status="SUCCEEDED")
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
