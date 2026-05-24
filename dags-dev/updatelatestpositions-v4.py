from datetime import datetime, timezone
import uuid
import os
import logging

from updatelatestpositions.orchestration_dependencies import (
    UpdateLatestPositionsOrchestrationDependencies,
    get_updatelatestpositions_orchestration_dependencies,
)
from updatelatestpositions.config.updatelatestpositions_config_schema import (
    GeneralConfig,
)
from updatelatestpositions.domain.logger import UpdateLatestPositionsLogger
from quality.execution_phase_metrics import ExecutionPhaseMetricsTracker
from observability.structured_event_logger import get_structured_logger, set_execution_context

PIPELINE_NAME = "updatelatestpositions"
PHASE_ORDER = ["config_load", "update_latest_positions"]
_IN_AIRFLOW = bool(os.getenv("AIRFLOW_HOME"))

if _IN_AIRFLOW:
    VERSION = 4
    DAG_NAME = f"{PIPELINE_NAME}-v{VERSION}"
else:
    from logging.handlers import RotatingFileHandler

    LOG_FILENAME = f"{PIPELINE_NAME}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            RotatingFileHandler(LOG_FILENAME, maxBytes=5 * 1024 * 1024, backupCount=5),
            logging.StreamHandler(),
        ],
    )


def update_latest_positions_table(
    deps: UpdateLatestPositionsOrchestrationDependencies | None = None,
) -> None:
    if deps is None:
        deps = get_updatelatestpositions_orchestration_dependencies()

    execution_id = str(uuid.uuid4())
    run_ts = datetime.now(timezone.utc)
    structured_logger = UpdateLatestPositionsLogger(
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
    set_execution_context(execution_id, correlation_id=execution_id)
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

    tracker.begin("update_latest_positions")
    try:
        deps.create_latest_positions(pipeline_config)
        tracker.finish("update_latest_positions", "success")
    except Exception as exc:
        tracker.finish("update_latest_positions", "failed")
        structured_logger.error(
            event="execution_phase_metrics",
            message="Execution phase metrics",
            status="FAILED",
            metadata=tracker.to_log_payload("failed"),
        )
        structured_logger.error(
            event="execution_aborted",
            message="Pipeline aborted: update latest positions failed",
            status="FAILED",
            metadata={"phase": "update_latest_positions"},
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

    TRANSFORMED_POSITIONS_READY_SIGNAL = Dataset("sptrans://trusted/transformed_positions_ready")
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": days_ago(0),
        "max_active_runs": 1,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
    }

    with DAG(
        DAG_NAME,
        default_args=default_args,
        description="Load latest positions to refined.latest_positions table",
        schedule=[TRANSFORMED_POSITIONS_READY_SIGNAL],
        catchup=False,
        tags=["sptrans"],
    ) as dag:
        update_latest_positions_task = PythonOperator(
            task_id="update_to_db",
            python_callable=update_latest_positions_table,
        )

        update_latest_positions_task
else:
    def main():
        update_latest_positions_table()

    if __name__ == "__main__":
        main()
