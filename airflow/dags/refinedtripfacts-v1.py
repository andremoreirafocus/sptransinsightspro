from refinedtripfacts.domain.logger import RefinedTripFactsLogger
from observability.structured_event_logger import get_structured_logger
import os
import logging

PIPELINE_NAME = "refinedtripfacts"
_IN_AIRFLOW = bool(os.getenv("AIRFLOW_HOME"))

if _IN_AIRFLOW:
    VERSION = 1
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

    from refinedtripfacts.build_trip_facts import build_trip_facts

    FINISHEDTRIPS_READY_SIGNAL = Dataset("sptrans://refined/finished_trips_ready")
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": days_ago(0),
        "max_active_runs": 1,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
    }

    def build_trip_facts_airflow_wrapper(triggering_dataset_events):
        events = triggering_dataset_events.get(FINISHEDTRIPS_READY_SIGNAL.uri, [])
        raw_payload = events[0].extra if events else {}
        logic_date_str = raw_payload.get("logical_date_string") if raw_payload else None
        airflow_logger = RefinedTripFactsLogger(
            get_structured_logger(service=PIPELINE_NAME, component="orchestrator", logger_name=__name__)
        )
        airflow_logger.info(
            event="dataset_trigger_received",
            message="Dataset trigger received from sptrans://refined/finished_trips_ready",
            metadata={"payload": raw_payload},
        )
        build_trip_facts(logic_date_str=logic_date_str)

    with DAG(
        DAG_NAME,
        default_args=default_args,
        description="Build trip_facts dimensional table from refined finished trips",
        schedule=[FINISHEDTRIPS_READY_SIGNAL],
        catchup=False,
        tags=["sptrans"],
    ) as dag:
        build_trip_facts_task = PythonOperator(
            task_id="build_trip_facts",
            python_callable=build_trip_facts_airflow_wrapper,
        )
        build_trip_facts_task
else:
    from refinedtripfacts.build_trip_facts import build_trip_facts

    def main():
        year = 2026
        month = 6
        day = 18
        hour = 21
        minute = 18
        test_logic_date = f"{year}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:00+00:00"
        build_trip_facts(logic_date_str=test_logic_date)

    if __name__ == "__main__":
        main()
