import logging
import os

from transformlivedata.transformlivedata import load_transform_save_positions

PIPELINE_NAME = "transformlivedata"
_IN_AIRFLOW = bool(os.getenv("AIRFLOW_HOME"))

if _IN_AIRFLOW:
    VERSION = 9
    DAG_NAME = f"{PIPELINE_NAME}-v{VERSION}"
else:
    from logging.handlers import RotatingFileHandler

    LOG_FILENAME = f"{PIPELINE_NAME}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            RotatingFileHandler(LOG_FILENAME, maxBytes=5 * 1024 * 1024, backupCount=5),
            logging.StreamHandler(),
        ],
    )

logger = logging.getLogger(__name__)


def transform_positions(logical_date_string: str) -> None:
    load_transform_save_positions(PIPELINE_NAME, logical_date_string)


if _IN_AIRFLOW:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": days_ago(20),
        "max_active_runs": 1,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
    }

    def _transform_positions_task(**context):
        logical_date = context["dag_run"].logical_date
        logical_date_string = logical_date.isoformat()
        transform_positions(logical_date_string)

    with DAG(
        DAG_NAME,
        default_args=default_args,
        description="Load data from raw layer, process it, and store it in trusted layer",
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["sptrans"],
    ) as dag:
        transform_positions_task = PythonOperator(
            task_id="transform_positions",
            python_callable=_transform_positions_task,
        )

        transform_positions_task
else:
    def main():
        logical_date_string = "2026-04-27T15:14:00+00:00"
        result = transform_positions(logical_date_string)
        logger.info(f"Pipeline result: {result}")

    if __name__ == "__main__":
        main()
