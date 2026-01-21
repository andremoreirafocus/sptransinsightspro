from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from refinedfinishedtrips.services.extract_trips_for_all_Lines_and_vehicles_db import (
    load_all_lines_and_vehicles_last_3_hours,
    generate_trips_for_all_Lines_and_vehicles,
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
    # "retries": 0,
}


# Função que combina todas as etapas do pipeline de ingestão de dados
def get_lines_and_vehicles():
    config = get_config()
    logging.info("Loding lines and vehicles...")
    all_lines_and_vehicles = load_all_lines_and_vehicles_last_3_hours(config)
    if not all_lines_and_vehicles:
        logging.error("No position data found to extract trips.")
        raise ValueError("No position data found to extract trips.")
    logging.info(
        f"Succesfully got {len(all_lines_and_vehicles)} line and vehicle combinations."
    )
    return all_lines_and_vehicles


def generate_trips(ti):
    config = get_config()
    all_lines_and_vehicles = ti.xcom_pull(task_ids="get_lines_and_vehicles")
    logging.info(
        f"Received exactly {all_lines_and_vehicles} line and vehicle combinations."
    )
    logging.info(
        f"Received {len(all_lines_and_vehicles)} line and vehicle combinations."
    )
    generate_trips_for_all_Lines_and_vehicles(config, all_lines_and_vehicles)


with DAG(
    "refinefinishedtrips-v1",
    default_args=default_args,
    description="Load raw data from MinIO, process it, and store it in PG",
    schedule_interval="*/15 * * * * *",  # Use cron expression for every minute
    catchup=False,
) as dag:
    get_lines_and_vehicles_task = PythonOperator(
        task_id="get_lines_and_vehicles", python_callable=get_lines_and_vehicles
    )

    generate_trips_task = PythonOperator(
        task_id="generate_trips",
        python_callable=generate_trips,
    )

    get_lines_and_vehicles_task >> generate_trips_task
