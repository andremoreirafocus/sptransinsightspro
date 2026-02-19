from datetime import timedelta
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

from refinedsynctripdetails.services.load_trip_details_from_storage_to_dataframe import (
    load_trip_details_from_storage_to_dataframe,
)
from refinedsynctripdetails.services.save_trip_details_from_dataframe_to_refined import (
    save_trip_details_from_dataframe_to_refined,
)
from refinedsynctripdetails.config import get_config
from datetime import timedelta
# import logging

TRIP_DATA_SIGNAL = Dataset("gtfs://trip_details_ready")
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
    "refinedsynctripdetails-v2",
    default_args=default_args,
    schedule=[TRIP_DATA_SIGNAL],
    catchup=False,
    tags=["sptrans"],
) as dag:
    refined_sync_trip_details_task = PythonOperator(
        task_id="refined_sync_trip_details", python_callable=refined_sync_trip_details
    )

    refined_sync_trip_details_task
