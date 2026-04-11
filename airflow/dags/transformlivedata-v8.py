from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from transformlivedata.transformlivedata import load_transform_save_positions

import logging

logger = logging.getLogger(__name__)

PIPELINE_NAME = "transformlivedata"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(20),
    "max_active_runs": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


# def load_transform_save_positions(logical_date_string, **kwargs):
def transform_positions(**context):
    logical_date = context["dag_run"].logical_date
    logical_date_string = logical_date.isoformat()
    load_transform_save_positions(PIPELINE_NAME, logical_date_string)


# Criando o DAG
with DAG(
    "transformlivedata-v8",
    default_args=default_args,
    description="Load data from raw layer, process it, and store it in trusted layer",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["sptrans"],
) as dag:
    transform_positions_task = PythonOperator(
        task_id="transform_positions",
        python_callable=transform_positions,
        # op_kwargs={"logical_date_string": "{{ ts }}"},
    )

    transform_positions_task
