from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import DagRun
from airflow.models.dagrun import DagRunType, DagRunState
from airflow.utils.db import create_session
from airflow.utils import timezone
from orchestratetransform.services.processed_requests_helper import (
    get_unprocessed_requests,
)
from orchestratetransform.config import get_config
import uuid
from datetime import datetime
from sqlalchemy import and_
import time
import logging

logger = logging.getLogger(__name__)

# Definindo os argumentos padrão para as tarefas do DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(0),
    "max_active_runs": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
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

    # Convert logical_date to datetime object and ISO string
    if isinstance(logical_date, datetime):
        logical_date_dt = logical_date
        logical_date_str = logical_date.isoformat()
    else:
        logical_date_str = str(logical_date)
        logical_date_dt = datetime.fromisoformat(logical_date_str)

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
                        return False
                    else:
                        logger.info(
                            f"Run with run_id: {existing_run.run_id} is in state: {existing_run.state}. "
                            f"Creating a new run anyway."
                        )

                session.commit()

        # Create a unique run_id for this DAG execution
        run_id = f"manual__{timezone.utcnow().strftime('%Y%m%d_%H%M%S')}__{uuid.uuid4().hex[:8]}"
        logger.info(f"Generated run_id for DAG: {run_id}")

        # Create a new DagRun for transformlivedata-v4
        dag_run = DagRun(
            dag_id=dag_name,
            run_id=run_id,
            execution_date=logical_date_dt,
            start_date=logical_date_dt,
            external_trigger=True,
            run_type=DagRunType.MANUAL,
            conf={"logical_date_string": logical_date_str},
        )

        # Add the DagRun to the database
        with create_session() as session:
            session.add(dag_run)
            session.commit()

        logger.info(
            f"DAG for unprocessed request with logical_date: {logical_date} started successfully! "
            f"(run_id: {run_id})"
        )
        return True

    except Exception as e:
        logger.error(
            f"Failed to trigger DAG for logical_date {logical_date}: {e}", exc_info=True
        )
        return False


def trigger_dag_for_unprocessed_requests():
    config = get_config()

    # Get configuration values from config (Airflow Variables)
    def get_config_values(config):
        try:
            dag_name = config["ORCHESTRATE_TARGET_DAG"]
            wait_time_seconds = int(config["ORCHESTRATE_WAIT_TIME_SECONDS"])
            return dag_name, wait_time_seconds
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    dag_name, wait_time_seconds = get_config_values(config)
    # Wait for ingest service to complete file download, MinIO save, and database write
    # The ingest service takes time to: download JSON, save to MinIO, and save pending request to DB
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
            run_dag_for_unprocessed_request(dag_name, request["logical_date"])
    else:
        logger.info("No unprocessed requests found.")


with DAG(
    "orchestratetransform-v1",
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
