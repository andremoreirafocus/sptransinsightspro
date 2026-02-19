from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data_eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="postgres_partition_maintenance",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",  # Runs every hour
    catchup=False,
    tags=["refined", "maintenance"],
) as dag:
    # Trigger pg_partman to create new partitions and drop old ones
    run_partman_maintenance = SQLExecuteQueryOperator(
        task_id="run_pg_partman_maintenance",
        conn_id="postgres_conn",
        sql="SELECT partman.run_maintenance('refined.finished_trips');",
        autocommit=True,
    )
