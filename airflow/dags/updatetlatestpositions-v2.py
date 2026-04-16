from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from updatelatestpositions.services.create_latest_positions import (
    create_latest_positions_table,
)
from pipeline_configurator.config import get_config
from updatelatestpositions.config.updatelatestpositions_config_schema import (
    GeneralConfig,
)

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


def update_latest_positions_table():
    try:
        pipeline_config = get_config(
            "updatelatestpositions",
            None,
            GeneralConfig,
            None,
            "minio_conn",
            "postgres_conn",
        )
    except Exception as e:
        raise ValueError(f"Pipeline configuration validation failed: {e}")
    create_latest_positions_table(pipeline_config)


# Criando o DAG
with DAG(
    "updatelatestposition-v2",
    default_args=default_args,
    description="Load latest positions to refined.latest_positions table",
    schedule_interval="*/2 * * * *",  # Use cron expression for every minute
    catchup=False,
    tags=["sptrans"],
) as dag:
    update_latest_positions_task = PythonOperator(
        task_id="update_to_db",
        python_callable=update_latest_positions_table,
    )

    update_latest_positions_task
