import logging
import os

from gtfs.gtfs import (
    build_run_context,
    build_quality_report,
    create_trip_details,
    extract_load_files,
    transform,
)
from gtfs.orchestration_dependencies import get_gtfs_orchestration_dependencies

PIPELINE_NAME = "gtfs"
_IN_AIRFLOW = bool(os.getenv("AIRFLOW_HOME"))

if _IN_AIRFLOW:
    VERSION = 7
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


def extract_load_files_wrapper():
    deps = get_gtfs_orchestration_dependencies()
    run_context = build_run_context()
    stage_results = {}
    stage_results = extract_load_files(run_context, stage_results, deps=deps)
    return {"run_context": run_context, "stage_results": stage_results}


def transform_wrapper(input):
    deps = get_gtfs_orchestration_dependencies()
    run_context = input["run_context"]
    stage_results = input["stage_results"]
    stage_results = transform(run_context, stage_results, deps=deps)
    return {"run_context": run_context, "stage_results": stage_results}


def create_trip_details_wrapper(input):
    deps = get_gtfs_orchestration_dependencies()
    run_context = input["run_context"]
    stage_results = input["stage_results"]
    stage_results = create_trip_details(run_context, stage_results, deps=deps)
    return {"run_context": run_context, "stage_results": stage_results}


def build_quality_report_wrapper(input):
    deps = get_gtfs_orchestration_dependencies()
    run_context = input["run_context"]
    stage_results = input["stage_results"]
    build_quality_report(run_context, stage_results, deps=deps)


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

    def transform_airflow_wrapper(ti):
        input = ti.xcom_pull(task_ids="extract_load_files")
        return transform_wrapper(input)

    def create_trip_details_airflow_wrapper(ti, outlet_events):
        input = ti.xcom_pull(task_ids="transform")
        result = create_trip_details_wrapper(input)
        outlet_events[TRIP_DATA_SIGNAL].extra = {
            "correlation_id": result["run_context"]["batch_ts"]
        }
        logging.getLogger(__name__).info(
            "Dataset outlet event published: correlation_id=%s",
            result["run_context"]["batch_ts"],
        )
        return result

    def build_quality_report_airflow_wrapper(ti):
        input = ti.xcom_pull(task_ids="create_trip_details")
        build_quality_report_wrapper(input)

    with DAG(
        DAG_NAME,
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
            task_id="transform", python_callable=transform_airflow_wrapper
        )

        create_trip_details_task = PythonOperator(
            task_id="create_trip_details",
            python_callable=create_trip_details_airflow_wrapper,
            outlets=[TRIP_DATA_SIGNAL],
        )

        build_quality_report_task = PythonOperator(
            task_id="build_quality_report",
            python_callable=build_quality_report_airflow_wrapper,
        )

        (
            extract_load_files_task
            >> transform_task
            >> create_trip_details_task
            >> build_quality_report_task
        )
else:
    def main():
        output = extract_load_files_wrapper()
        output = transform_wrapper(output)
        output = create_trip_details_wrapper(output)
        build_quality_report_wrapper(output)

    if __name__ == "__main__":
        main()
