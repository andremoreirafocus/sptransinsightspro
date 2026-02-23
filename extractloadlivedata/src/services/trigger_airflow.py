import os
import requests

from datetime import datetime
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)

# Persistent Volume Paths
NOTIF_DIR = "../ingest_notifications"


def map_storage_filename_to_marker_name(filename):
    # Assuming filename is like 'posicoes_onibus-202602201400.json'
    # We want to extract '202602201400' to create '202602201400.pending'
    marker_name = f"{filename.split('.')[0]}.pending"
    marker_fullpath_name = os.path.join(NOTIF_DIR, marker_name)
    logger.info(
        f"Mapping storage filename '{filename}' to marker name '{marker_name}' and full path '{marker_fullpath_name}'"
    )
    return marker_name, marker_fullpath_name


def create_pending_invokation(filename):
    logger.info(f"Creating pending invokation for file '{filename}'")
    _, marker_fullpath_name = map_storage_filename_to_marker_name(filename)
    with open(marker_fullpath_name, "w") as f:
        f.write(filename)  # Keep the reference inside
    logger.info(
        f"Pending invokation created at '{marker_fullpath_name}' with content '{filename}'"
    )


def remove_pending_invokation(marker_name):
    marker_fullpath_name = os.path.join(NOTIF_DIR, marker_name)
    logger.info(f"Removing pending invokation marker '{marker_fullpath_name}'")
    os.remove(marker_fullpath_name)
    logger.info(
        f"Pending invokation marker '{marker_fullpath_name}' removed successfully."
    )


def get_pending_invokations():
    logger.info("Checking for pending invokations...")
    pending_markers = sorted(os.listdir(NOTIF_DIR))
    logger.info(f"Found {len(pending_markers)} pending invokation(s).")
    return pending_markers


def trigger_pending_invokations():
    pending_markers = get_pending_invokations()
    if pending_markers:
        for pending_marker in pending_markers:
            print(f"Pending invokation found: {pending_marker}")
            print(f"Found {len(pending_markers)} pending invokation(s). Processing...")
            if trigger_airflow_push(pending_marker):
                remove_pending_invokation(pending_marker)
    else:
        print("No pending invokations found.")


def trigger_airflow_push(pending_marker):
    """
    Sends the POST request to Airflow API using logical_date.
    """
    # Convert '202602201400' to ISO format for Airflow
    logger.info(f"Triggering Airflow DAG for : {pending_marker}")
    print(f"pending_marker: {pending_marker}")
    # filename = "posicoes_onibus-202602231750.pending"

    # Find the part between '-' and '.'
    timestamp = pending_marker.split("-")[1].split(".")[0]

    # Slice by index
    year = timestamp[0:4]
    month = timestamp[4:6]
    day = timestamp[6:8]
    hour = timestamp[8:10]
    minute = timestamp[10:12]

    logical_date = f"{year}-{month}-{day}T{hour}:{minute}:00Z"
    dt_obj = datetime(int(year), int(month), int(day), int(hour), int(minute))

    # 2. Format as ISO 8601 for Airflow (YYYY-MM-DDTHH:MM:SSZ)
    logical_date = dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"logical_date: {logical_date}")
    # Output: 2026-02-23T14:00:00Z
    payload = {
        "logical_date": logical_date,
        "conf": {
            "origin": "exctractloadlivedata",
            "marker_file": pending_marker,
        },
        "note": "Triggered by ingest microservice after successful Raw upload",
    }
    print(f"payload: {payload}")
    dag_name = "transformlivedata-v5"
    # airflow_webserver = "airflow-webserver"  # Use the service name defined in docker-compose.yml
    airflow_webserver = (
        "localhost"  # Use the service name defined in docker-compose.yml
    )
    try:
        r = requests.post(
            f"http://{airflow_webserver}:8080/api/v1/dags/{dag_name}/dagRuns",
            json=payload,
            auth=("admin", "admin"),
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
