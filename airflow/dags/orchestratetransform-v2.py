from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import DagRun, DagBag
from airflow.models.dagrun import DagRunState
from airflow.api.common.trigger_dag import trigger_dag
from airflow.utils.db import create_session
from airflow.utils import timezone
from airflow.exceptions import DagRunAlreadyExists
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
                    elif existing_run.state == DagRunState.SUCCESS:
                        logger.info(
                            f"Run with run_id: {existing_run.run_id} is in state: {existing_run.state}. "
                            "Skipping new trigger for the same logical_date."
                        )
                        return "SKIPPED_EXISTING"
                    else:
                        logger.info(
                            "Run with run_id: %s is in terminal state %s. "
                            "Skipping new trigger for the same logical_date.",
                            existing_run.run_id,
                            existing_run.state,
                        )
                        return "SKIPPED_EXISTING"
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
    except DagRunAlreadyExists as e:
        logger.info(
            "DAG run already exists for logical_date %s. Skipping trigger. Details: %s",
            logical_date,
            e,
        )
        return "SKIPPED_EXISTING"

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

    metrics = {
        "pending_requests_total": 0,
        "pending_logical_dates_total": 0,
        "target_dag_state_counts": {
            "no_run": 0,
            "queued": 0,
            "running": 0,
            "success": 0,
            "failed": 0,
            "other": 0,
        },
        "trigger_outcomes": {
            "triggered": 0,
            "skipped_running": 0,
            "skipped_existing": 0,
            "failed": 0,
        },
    }

    def _normalize_state(state):
        if state == DagRunState.QUEUED:
            return "queued"
        if state == DagRunState.RUNNING:
            return "running"
        if state == DagRunState.SUCCESS:
            return "success"
        if state == DagRunState.FAILED:
            return "failed"
        return "other"

    def _count_existing_run_states_for_logical_dates(dag_id, logical_dates):
        if not logical_dates:
            return
        with create_session() as session:
            existing_runs = (
                session.query(DagRun)
                .filter(
                    and_(
                        DagRun.dag_id == dag_id,
                        DagRun.execution_date.in_(logical_dates),
                    )
                )
                .all()
            )

        by_logical_date = {}
        for run in existing_runs:
            by_logical_date.setdefault(run.execution_date, []).append(run.state)

        for logical_date in logical_dates:
            states = by_logical_date.get(logical_date, [])
            if not states:
                metrics["target_dag_state_counts"]["no_run"] += 1
                continue
            for state in states:
                metrics["target_dag_state_counts"][_normalize_state(state)] += 1

    unprocessed_requests = get_unprocessed_requests(config)
    failure_reason = None
    try:
        if unprocessed_requests:
            metrics["pending_requests_total"] = len(unprocessed_requests)
            pending_logical_dates = sorted(
                {
                    request["logical_date"]
                    for request in unprocessed_requests
                    if request.get("logical_date")
                }
            )
            metrics["pending_logical_dates_total"] = len(pending_logical_dates)
            _count_existing_run_states_for_logical_dates(dag_name, pending_logical_dates)

            logger.info(f"Found {len(unprocessed_requests)} unprocessed requests.")
            for request in unprocessed_requests:
                logger.info(
                    f"Found request with filename: {request['filename']} and logical_date: {request['logical_date']}"
                )
                result = run_dag_for_unprocessed_request(
                    dag_name, request["logical_date"]
                )
                if result == "TRIGGERED":
                    metrics["trigger_outcomes"]["triggered"] += 1
                elif result == "SKIPPED_RUNNING":
                    metrics["trigger_outcomes"]["skipped_running"] += 1
                    logger.info(
                        "Skipping trigger because DAG is already running for logical_date: %s",
                        request["logical_date"],
                    )
                elif result == "SKIPPED_EXISTING":
                    metrics["trigger_outcomes"]["skipped_existing"] += 1
                    logger.info(
                        "Skipping trigger because a DAG run already exists for logical_date: %s",
                        request["logical_date"],
                    )
                else:
                    metrics["trigger_outcomes"]["failed"] += 1
                    failure_reason = (
                        "Failed to trigger DAG for request with logical_date: "
                        f"{request['logical_date']}"
                    )
                    logger.error(failure_reason)
                    break
        else:
            logger.info("No unprocessed requests found.")
    finally:
        logger.info(
            "orchestratetransform execution summary | "
            "pending_requests_total=%s | pending_logical_dates_total=%s | "
            "target_dag_state_counts=%s | trigger_outcomes=%s",
            metrics["pending_requests_total"],
            metrics["pending_logical_dates_total"],
            metrics["target_dag_state_counts"],
            metrics["trigger_outcomes"],
        )

    if failure_reason:
        raise RuntimeError(failure_reason)


with DAG(
    "orchestratetransform-v2",
    default_args=default_args,
    description="Orchestrate transform for unprocessed requests",
    schedule_interval="*/2 * * * *",  # Use cron expression for every minute
    max_active_runs=1,
    catchup=False,
    tags=["sptrans"],
) as dag:
    trigger_dag_for_unprocessed_requests_task = PythonOperator(
        task_id="trigger_unprocessed_requests",
        python_callable=trigger_dag_for_unprocessed_requests,
    )

    trigger_dag_for_unprocessed_requests_task
