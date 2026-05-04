import logging
import os
from datetime import datetime, timedelta, timezone

from transformlivedata.transformlivedata import load_transform_save_positions

PIPELINE_NAME = "transformlivedata"
_IN_AIRFLOW = bool(os.getenv("AIRFLOW_HOME"))
VERSION = 9
BACKFILL_START = datetime(2026, 5, 1, 19, 10, 0, tzinfo=timezone.utc)
BACKFILL_END = datetime(2026, 5, 1, 22, 12, 0, tzinfo=timezone.utc)
BACKFILL_STEP_MINUTES = 2

if _IN_AIRFLOW:
    DAG_NAME = f"{PIPELINE_NAME}-backfill-v{VERSION}"
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

logger = logging.getLogger(__name__)


def transform_positions(logical_date_string: str) -> None:
    load_transform_save_positions(PIPELINE_NAME, logical_date_string)


if _IN_AIRFLOW:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": BACKFILL_START,
        "end_date": BACKFILL_END,
        "max_active_runs": 1,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
    }

    def _transform_positions_task(**context):
        logical_date = context["dag_run"].logical_date
        logical_date_string = logical_date.isoformat()
        transform_positions(logical_date_string)

    with DAG(
        DAG_NAME,
        default_args=default_args,
        description="One-time backfill of the last 3 hours of positions with corrected trip_details",
        schedule=f"*/{BACKFILL_STEP_MINUTES} * * * *",
        catchup=True,
        max_active_runs=1,
        tags=["sptrans", "backfill"],
    ) as dag:
        transform_positions_task = PythonOperator(
            task_id="transform_positions",
            python_callable=_transform_positions_task,
        )

        transform_positions_task
else:
    def main():
        current_logical_date = BACKFILL_START
        total_runs = 0
        while current_logical_date <= BACKFILL_END:
            logical_date_string = current_logical_date.isoformat()
            logger.info(f"Running local backfill for logical date {logical_date_string}")
            transform_positions(logical_date_string)
            total_runs += 1
            current_logical_date += timedelta(minutes=BACKFILL_STEP_MINUTES)

        logger.info(f"Local backfill finished after {total_runs} runs")

    if __name__ == "__main__":
        main()
