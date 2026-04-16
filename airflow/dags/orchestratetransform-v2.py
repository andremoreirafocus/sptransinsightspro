from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import DagRun, DagBag
from airflow.models.dagrun import DagRunState
from airflow.api.common.trigger_dag import trigger_dag
from airflow.utils.db import create_session
from airflow.utils import timezone
from orchestratetransform.services.processed_requests_helper import (
    get_unprocessed_requests,
)
from pipeline_configurator.config import get_config
from orchestratetransform.config.orchestratetransform_config_schema import (
    GeneralConfig,
)
import uuid
from datetime import datetime
from sqlalchemy import and_
import time
import logging

logger = logging.getLogger(__name__)
PIPELINE_NAME = "orchestratetransform"


def _load_pipeline_config():
    try:
        pipeline_config = get_config(
            PIPELINE_NAME,
            None,
            GeneralConfig,
            None,
            None,
            "airflow_postgres_conn",
        )
    except Exception as e:
        logger.error(f"Pipeline configuration validation failed: {e}")
        raise ValueError(f"Pipeline configuration validation failed: {e}")
    return pipeline_config


# Definindo os argumentos padrão para as tarefas do DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(0),
    "max_active_runs": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def run_dag_for_unprocessed_request(dag_name, logical_date):
    """
    Trigger the dag_name DAG for a specific logical_date.

    If a previous run for the same logical_date exists and failed, it will be
    cleared before creating a new run. This prevents duplicate runs and allows
    automatic recovery from failures.

    Args:
        logical_date: A datetime object or ISO format string representing the logical_date
                     of the unprocessed request

    Returns:
        bool: True if DAG was triggered successfully, False otherwise
    """
    logger.info(
        f"Starting DAG for unprocessed request with logical_date: {logical_date}..."
    )
    if isinstance(logical_date, datetime):
        logical_date_dt = logical_date
        logical_date_str = logical_date.isoformat()
    else:
        logical_date_str = str(logical_date)
        logical_date_dt = datetime.fromisoformat(logical_date_str)

        dagbag = DagBag(read_dags_from_db=True)
        if dag_name not in dagbag.dags:
            logger.error("Target DAG not found: %s", dag_name)
            return False
    try:
        # Check for existing failed DAG runs with the same logical_date
        with create_session() as session:
            existing_runs = (
                session.query(DagRun)
                .filter(
                    and_(
                        DagRun.dag_id == dag_name,
                        DagRun.execution_date == logical_date_dt,
                    )
                )
                .all()
            )
            if existing_runs:
                logger.info(
                    f"Found {len(existing_runs)} existing run(s) for logical_date: {logical_date}"
                )
                for existing_run in existing_runs:
                    if existing_run.state in [
                        DagRunState.FAILED,
                    ]:
                        logger.info(
                            f"Clearing failed run with run_id: {existing_run.run_id} (state: {existing_run.state})"
                        )
                        # Delete the failed run to allow a fresh trigger
                        session.delete(existing_run)
                    elif existing_run.state in [
                        DagRunState.RUNNING,
                        DagRunState.QUEUED,
                    ]:
                        logger.info(
                            f"Run with run_id: {existing_run.run_id} is already {existing_run.state}. Skipping new trigger."
                        )
                        return "SKIPPED_RUNNING"
                    else:
                        logger.info(
                            f"Run with run_id: {existing_run.run_id} is in state: {existing_run.state}. "
                            f"Creating a new run anyway."
                        )
                session.commit()
        logger.info(
            "Triggering DAG '%s' for logical_date: %s", dag_name, logical_date_str
        )
        trigger_dag(
            dag_id=dag_name,
            run_id=f"manual__{timezone.utcnow().strftime('%Y%m%d_%H%M%S')}__{uuid.uuid4().hex[:8]}",
            conf={"logical_date_string": logical_date_str},
            execution_date=logical_date_dt,
            replace_microseconds=False,
        )
        logger.info(
            f"DAG for unprocessed request with logical_date: {logical_date} started successfully! "
            f"(dag_id: {dag_name})"
        )
        return "TRIGGERED"

    except Exception as e:
        logger.error(
            f"Failed to trigger DAG for logical_date {logical_date}: {e}", exc_info=True
        )
        return "FAILED"


def trigger_dag_for_unprocessed_requests():
    config = _load_pipeline_config()

    def get_config_values(config):
        try:
            general = config["general"]
            orchestration = general["orchestration"]
            dag_name = orchestration["target_dag"]
            wait_time_seconds = int(orchestration["wait_time_seconds"])
            return dag_name, wait_time_seconds
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    dag_name, wait_time_seconds = get_config_values(config)
    logger.info(
        f"Waiting {wait_time_seconds} seconds for ingest service to complete file processing..."
    )
    time.sleep(wait_time_seconds)
    unprocessed_requests = get_unprocessed_requests(config)
    if unprocessed_requests:
        logger.info(f"Found {len(unprocessed_requests)} unprocessed requests.")
        for request in unprocessed_requests:
            logger.info(
                f"Found request with filename: {request['filename']} and logical_date: {request['logical_date']}"
            )
            result = run_dag_for_unprocessed_request(dag_name, request["logical_date"])
            if result == "FAILED":
                logger.error(
                    f"Failed to trigger DAG for request with logical_date: {request['logical_date']}"
                )
                raise RuntimeError(
                    f"Failed to trigger DAG for request with logical_date: {request['logical_date']}"
                )
            if result == "SKIPPED_RUNNING":
                logger.info(
                    "Skipping trigger because DAG is already running for logical_date: %s",
                    request["logical_date"],
                )
    else:
        logger.info("No unprocessed requests found.")


with DAG(
    "orchestratetransform-v2",
    default_args=default_args,
    description="Orchestrate transform for unprocessed requests",
    schedule_interval="*/2 * * * *",  # Use cron expression for every minute
    catchup=False,
    tags=["sptrans"],
) as dag:
    trigger_dag_for_unprocessed_requests_task = PythonOperator(
        task_id="trigger_unprocessed_requests",
        python_callable=trigger_dag_for_unprocessed_requests,
    )

    trigger_dag_for_unprocessed_requests_task
