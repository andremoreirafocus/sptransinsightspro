from orchestratetransform.services.processed_requests_helper import (
    get_unprocessed_requests,
)
from orchestratetransform.config import get_config
import time
import logging
from logging.handlers import RotatingFileHandler

LOG_FILENAME = "orchestratetransform.log"

# In Airflow just remove this logging configuration block
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        # Rotation: 5MB per file, keeping the last 5 files
        RotatingFileHandler(LOG_FILENAME, maxBytes=5 * 1024 * 1024, backupCount=5),
        logging.StreamHandler(),  # Also keeps console output
    ],
)

logger = logging.getLogger(__name__)


def run_dag_for_unprocessed_request(logical_date):
    logger.info(
        f"Starting DAG for unprocessed request with logical_date: {logical_date}..."
    )
    logger.info(
        f"DAG for unprocessed request with logical_date: {logical_date} started succesfully!"
    )


def trigger_dag_for_unprocessed_requests():
    config = get_config()

    # Get configuration values from config (Airflow Variables)
    def get_config_values(config):
        try:
            dag_name = config["ORCHESTRATE_TARGET_DAG"]
            wait_time_seconds = int(config["ORCHESTRATE_WAIT_TIME_SECONDS"])
            return dag_name, wait_time_seconds
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    dag_name, wait_time_seconds = get_config_values(config)
    time.sleep(wait_time_seconds)
    unprocessed_requests = get_unprocessed_requests(config)
    if unprocessed_requests:
        logger.info(f"Found {len(unprocessed_requests)} unprocessed requests.")
        for request in unprocessed_requests:
            logger.info(
                f"Found request with filename: {request['filename']} and logical_date: {request['logical_date']}"
            )
            run_dag_for_unprocessed_request(dag_name, request["logical_date"])
    else:
        logger.info("No unprocessed requests found.")


if __name__ == "__main__":
    trigger_dag_for_unprocessed_requests()
