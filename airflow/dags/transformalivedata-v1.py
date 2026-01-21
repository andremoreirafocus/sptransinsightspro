from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from transformlivedata.services.load_positions import load_positions
from transformlivedata.services.get_positions_table_from_raw import (
    get_positions_table_from_raw,
)
from transformlivedata.services.save_positions_to_db import save_positions_to_db
from transformlivedata.config import get_config

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
    # "retries": 0,
}


# Função que combina todas as etapas do pipeline de ingestão de dados
def get_raw_file():
    config = get_config()
    year = "2026"
    month = "01"
    day = "12"
    hour = "16"
    minute = "00"
    logging.info("Transforming position...")
    raw_positions = load_positions(config, year, month, day, hour, minute)
    if not raw_positions:
        logging.error("No position data found to transform.")
        raise ValueError("No position data found to transform.")
    return raw_positions

def convert_positions_to_in_memory_table(ti):
    raw_positions = ti.xcom_pull(task_ids="get_raw_file")
    positions_table = get_positions_table_from_raw(raw_positions)
    if not positions_table:
        logging.error("No valid position records found.")
        raise ValueError("No valid position records found.")
    return positions_table

def save_to_db(ti):
    config = get_config()
    positions_table = ti.xcom_pull(task_ids="convert_in_memory")
    save_positions_to_db(config, positions_table)

# Criando o DAG
with DAG(
    "transformalivedata",
    default_args=default_args,
    description="Load raw data from MinIO, process it, and store it in PG",
    schedule_interval="*/2 * * * *",  # Use cron expression for every minute
    catchup=False,
) as dag:
    get_file_task = PythonOperator(task_id="get_raw_file", python_callable=get_raw_file)

    convert_in_memory_task = PythonOperator(
        task_id="convert_in_memory",
        python_callable=convert_positions_to_in_memory_table,
    )

    save_to_db_task = PythonOperator(
        task_id="save_to_db",
        python_callable=save_to_db,
    )

    get_file_task >> convert_in_memory_task >> save_to_db_task
