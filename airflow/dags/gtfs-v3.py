from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from gtfs.services.extract_gtfs_files import extract_gtfs_files
from gtfs.services.save_files_to_raw_storage import save_files_to_raw_storage

from gtfs.services.create_save_trip_details import (
    create_trip_details_table_and_fill_missing_data,
)
from gtfs.services.transforms import (
    transform_calendar,
    transform_frequencies,
    transform_routes,
    transform_stop_times,
    transform_stops,
    transform_trips,
)
from gtfs.config_conns import get_config


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
def extract_load_files():
    config = get_config()
    files_list = extract_gtfs_files(config)
    save_files_to_raw_storage(config, files_list)


def transform():
    logging.info("Starting GTFS Transformations...")
    config = get_config()
    transform_routes(config)
    transform_trips(config)
    transform_stops(config)
    transform_stop_times(config)
    transform_frequencies(config)
    transform_calendar(config)


def create_trip_details():
    config = get_config()
    create_trip_details_table_and_fill_missing_data(config)


# Criando o DAG
with DAG(
    "gtfs-v3",
    default_args=default_args,
    description="Dowload data from GTFS, process it, and store it in PG",
    schedule_interval="1 0 * * *",  # Use cron expression for every minute
    catchup=False,
) as dag:
    extract_load_files_task = PythonOperator(
        task_id="extract_load_files", python_callable=extract_load_files
    )

    transform_task = PythonOperator(task_id="transform", python_callable=transform)

    create_trip_details_task = PythonOperator(
        task_id="create_trip_details", python_callable=create_trip_details
    )

    extract_load_files_task >> transform_task >> create_trip_details_task
