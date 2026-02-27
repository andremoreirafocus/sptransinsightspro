from datetime import datetime
from zoneinfo import ZoneInfo
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from transformlivedata.services.load_positions import load_positions
from transformlivedata.services.transform_positions import (
    transform_positions,
)
from transformlivedata.services.save_positions_to_storage import (
    save_positions_to_storage,
)
from transformlivedata.services.processed_requests_helper import (
    mark_request_as_processed,
)
from transformlivedata.config import get_config

import logging

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(0),
    "max_active_runs": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def load_transform_save_positions(logical_date_string, **kwargs):
    config = get_config()
    dt_utc = datetime.fromisoformat(logical_date_string)
    print("logical_date_string:", logical_date_string)
    dt = dt_utc.astimezone(ZoneInfo("America/Sao_Paulo"))
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    hour = dt.strftime("%H")
    minute = dt.strftime("%M")
    logging.info(f"Transforming position for {dt}...")
    raw_positions = load_positions(config, year, month, day, hour, minute)
    if not raw_positions:
        logging.error("No position data found to transform.")
        raise ValueError("No position data found to transform.")
    positions_table = transform_positions(config, raw_positions)
    if not positions_table:
        logging.error("No valid position records found after transformation.")
        raise ValueError("No valid position records found after transformation.")
    save_positions_to_storage(config, positions_table)
    mark_request_as_processed(config, logical_date_string)


# Criando o DAG
with DAG(
    "transformlivedata-v4",
    default_args=default_args,
    description="Load data from raw layer, process it, and store it in trusted layer",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["sptrans"],
) as dag:
    load_transform_save_positions_task = PythonOperator(
        task_id="transform_positions",
        python_callable=load_transform_save_positions,
        op_kwargs={"logical_date_string": "{{ ts }}"},
    )

    load_transform_save_positions_task
