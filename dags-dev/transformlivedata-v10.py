import logging
import os

from observability.structured_event_logger import get_structured_logger
from transformlivedata.domain.logger import TransformLivedataLogger
from transformlivedata.orchestration_dependencies import (
    get_transformlivedata_orchestration_dependencies,
)
from transformlivedata.transformlivedata import load_transform_save_positions

PIPELINE_NAME = "transformlivedata"
_IN_AIRFLOW = bool(os.getenv("AIRFLOW_HOME"))

if _IN_AIRFLOW:
    VERSION = 10
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
    deps = get_transformlivedata_orchestration_dependencies()
    load_transform_save_positions(PIPELINE_NAME, logical_date_string, deps)


if _IN_AIRFLOW:
    from airflow import DAG, Dataset
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago

    TRANSFORMED_POSITIONS_READY_SIGNAL = Dataset("sptrans://trusted/transformed_positions_ready")
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": days_ago(20),
        "max_active_runs": 1,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
    }

    def _transform_positions_task(outlet_events, **context):
        logical_date = context["dag_run"].logical_date
        logical_date_string = logical_date.isoformat()
        logger = TransformLivedataLogger(
            get_structured_logger(
                service=PIPELINE_NAME,
                component="orchestrator",
                logger_name=__name__,
            )
        )
        transform_positions(logical_date_string)
        outlet_events[TRANSFORMED_POSITIONS_READY_SIGNAL].extra = {
            "logical_date_string": logical_date_string
        }
        logger.info(
            event="dataset_outlet_published",
            message="Dataset outlet event published: sptrans://trusted/transformed_positions_ready",
            metadata={"correlation_id": logical_date_string},
        )

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
            outlets=[TRANSFORMED_POSITIONS_READY_SIGNAL],
        )

        transform_positions_task
else:
    def main():
        logical_date_string = "2026-05-16T16:08:00+00:00"
        result = transform_positions(logical_date_string)
        logger.info(f"Pipeline result: {result}")

    if __name__ == "__main__":
        main()
