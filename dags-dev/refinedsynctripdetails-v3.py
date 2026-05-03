from refinedsynctripdetails.services.load_trip_details_from_storage_to_dataframe import (
    load_trip_details_from_storage_to_dataframe,
)
from refinedsynctripdetails.services.save_trip_details_from_dataframe_to_refined import (
    save_trip_details_from_dataframe_to_refined,
)
from refinedsynctripdetails.services.transform_trip_details_for_refined import (
    transform_trip_details_for_refined,
)
from pipeline_configurator.config import get_config
from refinedsynctripdetails.config.refinedsynctripdetails_config_schema import (
    GeneralConfig,
)
import os
import logging

PIPELINE_NAME = "refinedsynctripdetails"
_IN_AIRFLOW = bool(os.getenv("AIRFLOW_HOME"))

if _IN_AIRFLOW:
    VERSION = 3
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


def refined_sync_trip_details():
    pipeline_config = _load_pipeline_config()
    df_trip_details = load_trip_details_from_storage_to_dataframe(pipeline_config)
    df_trip_details = transform_trip_details_for_refined(df_trip_details)
    save_trip_details_from_dataframe_to_refined(pipeline_config, df_trip_details)


if _IN_AIRFLOW:
    from airflow import DAG, Dataset
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago

    TRIP_DATA_SIGNAL = Dataset("gtfs://trip_details_ready")
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": days_ago(0),
        "max_active_runs": 1,
        "email_on_failure": False,
        "email_on_retry": False,
    }

    with DAG(
        DAG_NAME,
        default_args=default_args,
        schedule=[TRIP_DATA_SIGNAL],
        catchup=False,
        tags=["sptrans"],
    ) as dag:
        refined_sync_trip_details_task = PythonOperator(
            task_id="refined_sync_trip_details",
            python_callable=refined_sync_trip_details,
        )

        refined_sync_trip_details_task
else:
    def main():
        refined_sync_trip_details()


    if __name__ == "__main__":
        main()
