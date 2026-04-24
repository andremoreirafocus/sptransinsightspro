import requests
from datetime import datetime
from zoneinfo import ZoneInfo
import logging

from src.infra.cache import add_to_cache, get_from_cache, remove_from_cache
from src.services.exceptions import IngestNotificationError

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def create_pending_invokation(config, filename, cache_factory=None):
    """Add a pending invocation to the cache."""

    def get_config(config):
        cache_dir = config["INVOKATIONS_CACHE_DIR"]
        return cache_dir

    logger.info(f"Creating pending invokation for file '{filename}'")
    # Use filename as key, storing the filename as value
    marker_name = f"{filename.split('.')[0]}.pending"
    cache_dir = get_config(config)
    add_to_cache(cache_dir, marker_name, filename, cache_factory=cache_factory)

    logger.info(
        f"Pending invokation created in cache with key '{marker_name}' and value '{filename}'"
    )


def remove_pending_invokation(config, marker_name, cache_factory=None):
    """Remove a pending invocation from the cache."""

    def get_config(config):
        cache_dir = config["INVOKATIONS_CACHE_DIR"]
        return cache_dir

    logger.info(f"Removing pending invokation marker '{marker_name}'")
    cache_dir = get_config(config)
    remove_from_cache(cache_dir, marker_name, cache_factory=cache_factory)


def get_pending_invokations(config, cache_factory=None):
    """Retrieve all pending invocations from the cache."""

    def get_config(config):
        cache_dir = config["INVOKATIONS_CACHE_DIR"]
        return cache_dir

    logger.info("Checking for pending invokations...")
    cache_dir = get_config(config)
    pending_markers = get_from_cache(cache_dir, cache_factory=cache_factory)
    logger.info(f"Found {len(pending_markers)} pending invokation(s).")
    return pending_markers


def trigger_pending_airflow_dag_invokations(config, post_fn=None, cache_factory=None):
    """Trigger all pending invocations and remove them if successful."""
    pending_markers = get_pending_invokations(config, cache_factory=cache_factory)
    if pending_markers:
        for pending_marker in pending_markers:
            logger.info(f"Pending invokation found: {pending_marker}")
            logger.info(
                f"Found {len(pending_markers)} pending invokation(s). Processing..."
            )
            if trigger_airflow_dag_run(config, pending_marker, post_fn=post_fn):
                remove_pending_invokation(
                    config, pending_marker, cache_factory=cache_factory
                )
    else:
        logger.info("No pending invokations found.")


def get_utc_logical_date_from_file(pending_marker):
    current_timezone_name = datetime.now(ZoneInfo("localtime")).tzname()
    logger.info(f"Current timezone: {current_timezone_name}")
    logger.info(f"pending_marker : {pending_marker}")
    timestamp = pending_marker.split("-")[1].split(".")[0]
    year = timestamp[0:4]
    month = timestamp[4:6]
    day = timestamp[6:8]
    hour = timestamp[8:10]
    minute = timestamp[10:12]
    dt_obj = datetime(int(year), int(month), int(day), int(hour), int(minute))
    dt_obj = dt_obj.replace(tzinfo=ZoneInfo("America/Sao_Paulo"))
    dt_utc = dt_obj.astimezone(ZoneInfo("UTC"))
    logical_date = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    return logical_date


def trigger_airflow_dag_run(config, pending_marker, post_fn=None):
    """
    Sends the POST request to Airflow API using logical_date.
    """

    def get_config(config):
        user = config["AIRFLOW_USER"]
        password = config["AIRFLOW_PASSWORD"]
        airflow_webserver = config["AIRFLOW_WEBSERVER"]
        dag_name = config["AIRFLOW_DAG_NAME"]
        airflow_url = f"http://{airflow_webserver}:8080/api/v1/dags/{dag_name}/dagRuns"
        auth = (
            user,
            password,
        )

        return airflow_url, auth

    airflow_url, auth = get_config(config)
    logger.info(f"Airflow URL: {airflow_url}")
    logical_date = get_utc_logical_date_from_file(pending_marker)
    logger.info(
        f"Triggering Airflow DAG for : {pending_marker} file and logical_date: {logical_date}"
    )
    payload = {
        "logical_date": logical_date,
        "conf": {
            "origin": "exctractloadlivedata",
            "marker_file": pending_marker,
        },
        "note": "Triggered by microservice after successful Raw upload",
    }
    logger.info(f"Request payload to be submitted: {payload}")
    try:
        post_fn = post_fn or requests.post
        r = post_fn(
            airflow_url,
            json=payload,
            auth=auth,
            timeout=10,
        )
        logger.info(
            f"Airflow API response for marker '{pending_marker}': {r.status_code} - {r.text}"
        )
        # 200 = Success, 409 = Already exists (safe to proceed)
        if r.status_code in [200, 409, 201]:
            return True
        raise IngestNotificationError(
            f"airflow dag trigger failed for marker '{pending_marker}' with status {r.status_code}"
        )
    except Exception as e:
        logger.error(
            f"Error triggering Airflow DAG for marker '{pending_marker}':",
            exc_info=True,
        )
        logger.error(f"Exception details: {e}")
        if isinstance(e, IngestNotificationError):
            raise
        raise IngestNotificationError(
            f"airflow dag trigger failed for marker '{pending_marker}'"
        ) from e


def main():

    logical_date = get_utc_logical_date_from_file(
        "posicoes_onibus-202602241926.json.zst"
    )
    logger.info(f"Logical_date: {logical_date}")


if __name__ == "__main__":
    main()
