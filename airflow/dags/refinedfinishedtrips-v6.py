from refinedfinishedtrips.extract_trips import (
    extract_trips_for_all_Lines_and_vehicles,
)
import os
import logging

PIPELINE_NAME = "refinedfinishedtrips"
_IN_AIRFLOW = bool(os.getenv("AIRFLOW_HOME"))

if _IN_AIRFLOW:
    VERSION = 6
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


def extract_trips():
    extract_trips_for_all_Lines_and_vehicles(PIPELINE_NAME)


if _IN_AIRFLOW:
    from airflow import DAG, Dataset
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago

    TRANSFORMED_POSITIONS_READY_SIGNAL = Dataset("sptrans://trusted/transformed_positions_ready")
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": days_ago(0),
        "max_active_runs": 1,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
    }

    with DAG(
        DAG_NAME,
        default_args=default_args,
        description="Calculate finished trips for all lines and vehicles",
        schedule=[TRANSFORMED_POSITIONS_READY_SIGNAL],
        catchup=False,
        tags=["sptrans"],
    ) as dag:
        extract_trips_task = PythonOperator(
            task_id="extract_trips", python_callable=extract_trips
        )
        extract_trips_task
else:
    def main():
        extract_trips()

    if __name__ == "__main__":
        main()
