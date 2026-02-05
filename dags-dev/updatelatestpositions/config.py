import os


def get_config():
    # Check if we are running inside Airflow
    if os.getenv("AIRFLOW_HOME"):
        # Pulling from Airflow Variables
        # from airflow.models import Variable
        config = {
            "RAW_BUCKET": "raw",
            "TRUSTED_BUCKET": "trusted",
            "GTFS_FOLDER": "gtfs",
            "APP_FOLDER": "sptrans",
            "POSITIONS_TABLE_NAME": "positions",
            "TRIP_DETAILS_TABLE_NAME": "trip_details",
            "MINIO_ENDPOINT": "minio:9000",
            "ACCESS_KEY": "datalake",
            "SECRET_KEY": "datalake",
        }
        return config
    else:
        # Pulling from local .env or hardcoded defaults for testing
        from dotenv import dotenv_values

        return dotenv_values("updatelatestpositions/.env")
