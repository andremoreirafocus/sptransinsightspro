from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from gtfs.gtfs import (
    build_run_context,
    extract_load_files,
    transform,
    create_trip_details,
    build_quality_report_and_send_webhook,
)


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
PIPELINE_NAME = "gtfs"


def extract_load_files_wrapper():
    run_context = build_run_context()
    stage_results = {}
    stage_results = extract_load_files(run_context, stage_results)
    return {"run_context": run_context, "stage_results": stage_results}


def transform_wrapper(ti):
    input = ti.xcom_pull(task_ids="extract_load_files")
    run_context = input["run_context"]
    stage_results = input["stage_results"]
    stage_results = transform(run_context, stage_results)
    return {"run_context": run_context, "stage_results": stage_results}


def create_trip_details_wrapper(ti):
    input = ti.xcom_pull(task_ids="transform")
    run_context = input["run_context"]
    stage_results = input["stage_results"]
    stage_results = create_trip_details(run_context, stage_results)
    return {"run_context": run_context, "stage_results": stage_results}


def build_quality_report_wrapper(ti):
    input = ti.xcom_pull(task_ids="create_trip_details")
    run_context = input["run_context"]
    stage_results = input["stage_results"]
    build_quality_report_and_send_webhook(run_context, stage_results)


TRIP_DATA_SIGNAL = Dataset("gtfs://trip_details_ready")
# Criando o DAG
with DAG(
    "gtfs-v5",
    default_args=default_args,
    description="Dowload data from GTFS, process it, and store it in PG",
    schedule_interval="1 0 * * *",
    catchup=False,
    tags=["sptrans"],
) as dag:
    extract_load_files_task = PythonOperator(
        task_id="extract_load_files", python_callable=extract_load_files_wrapper
    )

    transform_task = PythonOperator(
        task_id="transform", python_callable=transform_wrapper
    )

    create_trip_details_task = PythonOperator(
        task_id="create_trip_details",
        python_callable=create_trip_details_wrapper,
        outlets=[TRIP_DATA_SIGNAL],
    )

    build_quality_report_task = PythonOperator(
        task_id="build_quality_report",
        python_callable=build_quality_report_wrapper,
    )

    (
        extract_load_files_task
        >> transform_task
        >> create_trip_details_task
        >> build_quality_report_task
    )
