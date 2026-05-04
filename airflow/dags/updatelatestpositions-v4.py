from updatelatestpositions.services.create_latest_positions import (
    create_latest_positions_table,
)
from pipeline_configurator.config import get_config
from updatelatestpositions.config.updatelatestpositions_config_schema import (
    GeneralConfig,
)
import os
import logging

PIPELINE_NAME = "updatelatestpositions"
_IN_AIRFLOW = bool(os.getenv("AIRFLOW_HOME"))

if _IN_AIRFLOW:
    VERSION = 4
    DAG_NAME = f"{PIPELINE_NAME}-v{VERSION}"
else:
    from logging.handlers import RotatingFileHandler

    LOG_FILENAME = f"{PIPELINE_NAME}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            RotatingFileHandler(
                LOG_FILENAME, maxBytes=5 * 1024 * 1024, backupCount=5
            ),
            logging.StreamHandler(),
        ],
    )

logger = logging.getLogger(__name__)


def _load_pipeline_config():
    try:
        pipeline_config = get_config(
            PIPELINE_NAME,
            None,
            GeneralConfig,
            None,
            "minio_conn",
            "postgres_conn",
        )
    except Exception as e:
        logger.error(f"Pipeline configuration validation failed: {e}")
        raise ValueError(f"Pipeline configuration validation failed: {e}")
    return pipeline_config


def update_latest_positions_table():
    pipeline_config = _load_pipeline_config()
    create_latest_positions_table(pipeline_config)


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
        description="Load latest positions to refined.latest_positions table",
        schedule=[TRANSFORMED_POSITIONS_READY_SIGNAL],
        catchup=False,
        tags=["sptrans"],
    ) as dag:
        update_latest_positions_task = PythonOperator(
            task_id="update_to_db",
            python_callable=update_latest_positions_table,
        )

        update_latest_positions_task
else:
    def main():
        update_latest_positions_table()

    if __name__ == "__main__":
        main()
