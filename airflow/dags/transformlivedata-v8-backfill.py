from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from transformlivedata.transformlivedata import load_transform_save_positions

import logging

logger = logging.getLogger(__name__)

PIPELINE_NAME = "transformlivedata"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 5, 1, 19, 10, 0, tzinfo=timezone.utc),
    "end_date": datetime(2026, 5, 1, 22, 12, 0, tzinfo=timezone.utc),
    "max_active_runs": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def transform_positions(**context):
    logical_date = context["dag_run"].logical_date
    logical_date_string = logical_date.isoformat()
    load_transform_save_positions(PIPELINE_NAME, logical_date_string)


with DAG(
    "transformlivedata-v8-backfill",
    default_args=default_args,
    description="One-time backfill of the last 3 hours of positions with corrected trip_details",
    schedule="*/2 * * * *",
    catchup=True,
    max_active_runs=1,
    tags=["sptrans", "backfill"],
) as dag:
    transform_positions_task = PythonOperator(
        task_id="transform_positions",
        python_callable=transform_positions,
    )

    transform_positions_task
