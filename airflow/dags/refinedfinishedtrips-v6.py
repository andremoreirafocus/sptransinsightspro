from refinedfinishedtrips.extract_trips import (
    extract_trips_for_all_Lines_and_vehicles,
)
from refinedfinishedtrips.domain.logger import RefinedFinishedTripsLogger
from observability.structured_event_logger import get_structured_logger
from datetime import datetime, timezone
import os
import uuid
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


if _IN_AIRFLOW:
    from airflow import DAG, Dataset
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago

    TRANSFORMED_POSITIONS_READY_SIGNAL = Dataset("sptrans://trusted/transformed_positions_ready")
    FINISHEDTRIPS_READY_SIGNAL = Dataset("sptrans://refined/finished_trips_ready")
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": days_ago(0),
        "max_active_runs": 1,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
    }

    def extract_trips_airflow_wrapper(triggering_dataset_events, outlet_events):
        events = triggering_dataset_events.get(TRANSFORMED_POSITIONS_READY_SIGNAL.uri, [])
        raw_payload = events[0].extra if events else {}
        logical_date_string = raw_payload.get("logical_date_string") if raw_payload else None
        correlation_id = logical_date_string
        logic_date_str = logical_date_string
        logger = RefinedFinishedTripsLogger(
            get_structured_logger(
                service=PIPELINE_NAME,
                component="orchestrator",
                logger_name=__name__,
            )
        )
        logger.info(
            event="dataset_trigger_received",
            message="Dataset trigger received from sptrans://trusted/transformed_positions_ready",
            metadata={"payload": raw_payload},
        )
        extract_trips_for_all_Lines_and_vehicles(
            PIPELINE_NAME,
            correlation_id=correlation_id,
            logic_date_str=logic_date_str,
        )
        outlet_events[FINISHEDTRIPS_READY_SIGNAL].extra = {"logical_date_string": logical_date_string}
        logger.info(
            event="dataset_outlet_published",
            message="Dataset outlet event published: sptrans://refined/finished_trips_ready",
            metadata={"correlation_id": correlation_id},
        )

    with DAG(
        DAG_NAME,
        default_args=default_args,
        description="Calculate finished trips for all lines and vehicles",
        schedule=[TRANSFORMED_POSITIONS_READY_SIGNAL],
        catchup=False,
        tags=["sptrans"],
    ) as dag:
        extract_trips_task = PythonOperator(
            task_id="extract_trips",
            python_callable=extract_trips_airflow_wrapper,
            outlets=[FINISHEDTRIPS_READY_SIGNAL],
        )
        extract_trips_task
else:
    def extract_trips():
        extract_trips_for_all_Lines_and_vehicles(
            PIPELINE_NAME,
            correlation_id=str(uuid.uuid4()),
            logic_date_str=datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat(),
        )
    
    def main():
        extract_trips()

    if __name__ == "__main__":
        main()
