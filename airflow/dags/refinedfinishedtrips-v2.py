from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from refinedfinishedtrips.services.extract_trips_for_all_Lines_and_vehicles_pandas import (
    extract_trips_for_all_Lines_and_vehicles_pandas as extract_trips_for_all_Lines_and_vehicles,
)

from refinedfinishedtrips.config import get_config

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
def extract_trips():
    config = get_config()
    extract_trips_for_all_Lines_and_vehicles(config)


with DAG(
    "refinefinishedtrips-v2",
    default_args=default_args,
    description="Calculate finished trips for all lines and vehicles",
    schedule_interval="*/15 * * * *",  # Use cron expression for every minute
    catchup=False,
    tags=["sptrans"],
) as dag:
    extract_trips_task = PythonOperator(
        task_id="extract_trips", python_callable=extract_trips
    )

    extract_trips_task
