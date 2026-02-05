from datetime import datetime
from zoneinfo import ZoneInfo
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from transformdatatotrusted.services.load_positions import load_positions
from transformdatatotrusted.services.transform_positions import (
    transform_positions,
)
from transformdatatotrusted.services.save_positions_to_db import save_positions_to_db
from transformdatatotrusted.config import get_config

# from time import time
# from datetime import datetime
import logging

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


# Função que combina todas as etapas do pipeline de ingestão de dados
def load_positions_from_raw(logical_date_string, **kwargs):
    config = get_config()
    dt_utc = datetime.fromisoformat(logical_date_string)
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
    return raw_positions


def transform_positions_to_in_memory_table(ti):
    config = get_config()
    raw_positions = ti.xcom_pull(task_ids="load_positions_from_raw")
    positions_table = transform_positions(config, raw_positions)
    if not positions_table:
        logging.error("No valid position records found after transformation.")
        raise ValueError("No valid position records found after transformation.")
    return positions_table


def save_to_db(ti):
    config = get_config()
    positions_table = ti.xcom_pull(task_ids="transform_positions_to_in_memory_table")
    save_positions_to_db(config, positions_table)


# Criando o DAG
with DAG(
    "transformdatatotrusted-v1",
    default_args=default_args,
    description="Load raw data from MinIO, process it, and store it in PG",
    schedule_interval="*/2 * * * *",  # Use cron expression for every minute
    catchup=False,
) as dag:
    load_from_raw_task = PythonOperator(
        task_id="load_positions_from_raw",
        python_callable=load_positions_from_raw,
        op_kwargs={"logical_date_string": "{{ ts }}"},
    )

    transform_positions_task = PythonOperator(
        task_id="transform_positions_to_in_memory_table",
        python_callable=transform_positions_to_in_memory_table,
    )

    save_to_db_trusted_task = PythonOperator(
        task_id="save_to_db",
        python_callable=save_to_db,
    )

    load_from_raw_task >> transform_positions_task >> save_to_db_trusted_task
