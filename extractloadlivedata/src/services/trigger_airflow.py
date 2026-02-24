import requests
import diskcache as dc
import os
from datetime import datetime
from zoneinfo import ZoneInfo
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)

# Diskcache directory for pending invocations
CACHE_DIR = "../.diskcache_pending_invocations"

# Initialize the cache
_cache = None


def get_cache():
    """Get or initialize the diskcache instance."""
    global _cache
    if _cache is None:
        os.makedirs(CACHE_DIR, exist_ok=True)
        _cache = dc.Cache(CACHE_DIR)
    return _cache


def create_pending_invokation(filename):
    """Add a pending invocation to the cache."""
    logger.info(f"Creating pending invokation for file '{filename}'")
    cache = get_cache()

    # Use filename as key, storing the filename as value
    marker_name = f"{filename.split('.')[0]}.pending"
    cache[marker_name] = filename

    logger.info(
        f"Pending invokation created in cache with key '{marker_name}' and value '{filename}'"
    )


def remove_pending_invokation(marker_name):
    """Remove a pending invocation from the cache."""
    logger.info(f"Removing pending invokation marker '{marker_name}'")
    cache = get_cache()
    if marker_name in cache:
        del cache[marker_name]
        logger.info(f"Pending invokation marker '{marker_name}' removed successfully.")
    else:
        logger.warning(f"Pending invokation marker '{marker_name}' not found in cache.")


def get_pending_invokations():
    """Retrieve all pending invocations from the cache."""
    logger.info("Checking for pending invokations...")
    cache = get_cache()
    pending_markers = sorted(list(cache))
    logger.info(f"Found {len(pending_markers)} pending invokation(s).")
    return pending_markers


def trigger_pending_airflow_dag_invokations(config):
    """Trigger all pending invocations and remove them if successful."""
    pending_markers = get_pending_invokations()
    if pending_markers:
        for pending_marker in pending_markers:
            print(f"Pending invokation found: {pending_marker}")
            print(f"Found {len(pending_markers)} pending invokation(s). Processing...")
            if trigger_airflow_dag_run(config, pending_marker):
                remove_pending_invokation(pending_marker)
    else:
        print("No pending invokations found.")


def get_utc_logical_date_from_file(pending_marker):
    timestamp = pending_marker.split("-")[1].split(".")[0]
    year = timestamp[0:4]
    month = timestamp[4:6]
    day = timestamp[6:8]
    hour = timestamp[8:10]
    minute = timestamp[10:12]
    dt_obj = datetime(int(year), int(month), int(day), int(hour), int(minute))
    dt_utc = dt_obj.astimezone(ZoneInfo("UTC"))
    logical_date = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    return logical_date


def trigger_airflow_dag_run(config, pending_marker):
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
        )  # Default Airflow credentials (should be secured in production)

        return airflow_url, auth

    airflow_url, auth = get_config(config)
    print(f"Airflow URL: {airflow_url}")
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
    print(f"payload: {payload}")
    try:
        r = requests.post(
            airflow_url,
            json=payload,
            auth=auth,
            timeout=10,
        )
        logger.info(
            f"Airflow API response for marker '{pending_marker}': {r.status_code} - {r.text}"
        )
        # 200 = Success, 409 = Already exists (safe to proceed)
        return r.status_code in [200, 409, 201]
    except Exception as e:
        logger.error(
            f"Error triggering Airflow DAG for marker '{pending_marker}':",
            exc_info=True,
        )
        logger.error("Exception details:", e)
        return False
