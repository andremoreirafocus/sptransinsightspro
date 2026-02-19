from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from refinedsynctripdetails.services.load_trip_details_from_storage_to_dataframe import (
    load_trip_details_from_storage_to_dataframe,
)
from refinedsynctripdetails.services.save_trip_details_from_dataframe_to_refined import (
    save_trip_details_from_dataframe_to_refined,
)
from refinedsynctripdetails.config import get_config
# import logging

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


def refined_sync_trip_details():
    config = get_config()
    df_trip_details = load_trip_details_from_storage_to_dataframe(config)
    save_trip_details_from_dataframe_to_refined(config, df_trip_details)


with DAG(
    "refinedsynctripdetails-v1",
    default_args=default_args,
    description="Sync trip details from trusted to refined layer",
    schedule_interval="10 0 * * *",  # Use cron expression for every minute
    catchup=False,
) as dag:
    refined_sync_trip_details_task = PythonOperator(
        task_id="refined_sync_trip_details", python_callable=refined_sync_trip_details
    )

    refined_sync_trip_details_task
