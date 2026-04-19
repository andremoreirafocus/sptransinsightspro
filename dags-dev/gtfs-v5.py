import logging
from logging.handlers import RotatingFileHandler

from gtfs.gtfs import (
    build_run_context,
    extract_load_files,
    transform,
    create_trip_details,
    build_quality_report_and_send_webhook,
)

LOG_FILENAME = "gtfs.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILENAME, maxBytes=5 * 1024 * 1024, backupCount=5),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


def extract_load_files_wrapper():
    run_context = build_run_context()
    stage_results = {}
    stage_results = extract_load_files(run_context, stage_results)
    return {"run_context": run_context, "stage_results": stage_results}


def transform_wrapper(input):
    run_context = input["run_context"]
    stage_results = input["stage_results"]
    stage_results = transform(run_context, stage_results)
    return {"run_context": run_context, "stage_results": stage_results}


def create_trip_details_wrapper(input):
    run_context = input["run_context"]
    stage_results = input["stage_results"]
    stage_results = create_trip_details(run_context, stage_results)
    return {"run_context": run_context, "stage_results": stage_results}


def build_quality_report_wrapper(input):
    run_context = input["run_context"]
    stage_results = input["stage_results"]
    build_quality_report_and_send_webhook(run_context, stage_results)


def main():
    output = extract_load_files_wrapper()
    output = transform_wrapper(output)
    output = create_trip_details_wrapper(output)
    build_quality_report_wrapper(output)


if __name__ == "__main__":
    main()
