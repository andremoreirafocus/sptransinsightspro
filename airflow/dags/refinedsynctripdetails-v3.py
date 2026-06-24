from datetime import datetime, timezone
import uuid
import os
import logging

from refinedsynctripdetails.orchestration_dependencies import (
    RefinedSyncTripDetailsOrchestrationDependencies,
    get_refinedsynctripdetails_orchestration_dependencies,
)
from refinedsynctripdetails.config.refinedsynctripdetails_config_schema import (
    GeneralConfig,
)
from refinedsynctripdetails.domain.logger import RefinedSyncTripDetailsLogger
from quality.execution_phase_metrics import ExecutionPhaseMetricsTracker
from observability.structured_event_logger import get_structured_logger, set_execution_context

PIPELINE_NAME = "refinedsynctripdetails"
PHASE_ORDER = [
    "config_load",
    "load_trip_details",
    "transform_trip_details",
    "save_trip_details",
]
_IN_AIRFLOW = bool(os.getenv("AIRFLOW_HOME"))

if _IN_AIRFLOW:
    VERSION = 3
    DAG_NAME = f"{PIPELINE_NAME}-v{VERSION}"
else:
    from logging.handlers import RotatingFileHandler

    LOG_FILENAME = f"{PIPELINE_NAME}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            RotatingFileHandler(
                LOG_FILENAME, maxBytes=5 * 1024 * 1024, backupCount=5
            ),
            logging.StreamHandler(),
        ],
    )


def refined_sync_trip_details(
    deps: RefinedSyncTripDetailsOrchestrationDependencies | None = None,
    correlation_id: str | None = None,
) -> None:
    if deps is None:
        deps = get_refinedsynctripdetails_orchestration_dependencies()

    execution_id = str(uuid.uuid4())
    run_ts = datetime.now(timezone.utc)
    structured_logger = RefinedSyncTripDetailsLogger(
        get_structured_logger(
            service=PIPELINE_NAME,
            component="orchestrator",
            logger_name=__name__,
        )
    )
    tracker = ExecutionPhaseMetricsTracker(
        pipeline=PIPELINE_NAME,
        execution_id=execution_id,
        logical_date_utc=run_ts.isoformat(),
        phase_order=PHASE_ORDER,
    )
    effective_correlation_id = correlation_id if correlation_id is not None else execution_id
    set_execution_context(execution_id, correlation_id=effective_correlation_id)
    structured_logger.info(
        event="execution_started",
        message="Starting execution",
        status="STARTED",
    )

    structured_logger.info(
        event="config_load_started",
        message="Loading pipeline configuration",
        status="STARTED",
    )
    tracker.begin("config_load")
    try:
        pipeline_config = deps.get_config(
            PIPELINE_NAME,
            None,
            GeneralConfig,
            None,
            "minio_conn",
            "postgres_conn",
        )
        tracker.finish("config_load", "success")
        structured_logger.info(
            event="config_load_succeeded",
            message="Configuration loaded successfully",
            status="SUCCEEDED",
        )
    except Exception as e:
        tracker.finish("config_load", "failed")
        structured_logger.error(
            event="execution_phase_metrics",
            message="Execution phase metrics",
            status="FAILED",
            metadata=tracker.to_log_payload("failed"),
        )
        structured_logger.error(
            event="execution_aborted",
            message="Pipeline aborted: config load failed",
            status="FAILED",
            metadata={"phase": "config_load"},
        )
        raise ValueError("Configuration load and validation failed") from e

    tracker.begin("load_trip_details")
    try:
        df_trip_details = deps.load_trip_details(pipeline_config)
        tracker.finish("load_trip_details", "success")
    except Exception as exc:
        tracker.finish("load_trip_details", "failed")
        structured_logger.error(
            event="execution_phase_metrics",
            message="Execution phase metrics",
            status="FAILED",
            metadata=tracker.to_log_payload("failed"),
        )
        structured_logger.error(
            event="execution_aborted",
            message="Pipeline aborted: load trip details failed",
            status="FAILED",
            metadata={"phase": "load_trip_details"},
        )
        raise

    tracker.begin("transform_trip_details")
    try:
        df_trip_details = deps.transform_trip_details(df_trip_details)
        tracker.finish("transform_trip_details", "success")
    except Exception as exc:
        tracker.finish("transform_trip_details", "failed")
        structured_logger.error(
            event="execution_phase_metrics",
            message="Execution phase metrics",
            status="FAILED",
            metadata=tracker.to_log_payload("failed"),
        )
        structured_logger.error(
            event="execution_aborted",
            message="Pipeline aborted: transform trip details failed",
            status="FAILED",
            metadata={"phase": "transform_trip_details"},
        )
        raise

    tracker.begin("save_trip_details")
    try:
        deps.save_trip_details(pipeline_config, df_trip_details)
        tracker.finish("save_trip_details", "success")
    except Exception as exc:
        tracker.finish("save_trip_details", "failed")
        structured_logger.error(
            event="execution_phase_metrics",
            message="Execution phase metrics",
            status="FAILED",
            metadata=tracker.to_log_payload("failed"),
        )
        structured_logger.error(
            event="execution_aborted",
            message="Pipeline aborted: save trip details failed",
            status="FAILED",
            metadata={"phase": "save_trip_details"},
        )
        raise

    structured_logger.info(
        event="execution_phase_metrics",
        message="Execution phase metrics",
        status="SUCCEEDED",
        metadata=tracker.to_log_payload("success"),
    )
    structured_logger.info(
        event="execution_finished",
        message="Pipeline run complete",
        status="SUCCEEDED",
    )


if _IN_AIRFLOW:
    from airflow import DAG, Dataset
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago

    TRIP_DATA_SIGNAL = Dataset("gtfs://trip_details_ready")
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": days_ago(0),
        "max_active_runs": 1,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,        
    }

    def refined_sync_trip_details_airflow_wrapper(triggering_dataset_events):
        events = triggering_dataset_events.get(TRIP_DATA_SIGNAL.uri, [])
        raw_payload = events[0].extra if events else {}
        correlation_id = raw_payload.get("correlation_id") if raw_payload else None
        logger = RefinedSyncTripDetailsLogger(
            get_structured_logger(
                service=PIPELINE_NAME,
                component="orchestrator",
                logger_name=__name__,
            )
        )
        logger.info(
            event="dataset_trigger_received",
            message="Dataset trigger received from gtfs://trip_details_ready",
            metadata={"payload": raw_payload},
        )
        refined_sync_trip_details(correlation_id=correlation_id)

    with DAG(
        DAG_NAME,
        default_args=default_args,
        schedule=[TRIP_DATA_SIGNAL],
        catchup=False,
        tags=["sptrans"],
    ) as dag:
        refined_sync_trip_details_task = PythonOperator(
            task_id="refined_sync_trip_details",
            python_callable=refined_sync_trip_details_airflow_wrapper,
        )

        refined_sync_trip_details_task
else:
    def main():
        refined_sync_trip_details()

    if __name__ == "__main__":
        main()
