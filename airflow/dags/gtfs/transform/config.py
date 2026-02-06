import os


def get_config():
    # Check if we are running inside Airflow
    if os.getenv("AIRFLOW_HOME"):
        # Pulling from Airflow Variables
        # from airflow.models import Variable
        config = {
            "RAW_BUCKET": "raw",
            "TRUSTED_BUCKET": "trusted",
            "APP_FOLDER": "sptrans",
            "MINIO_ENDPOINT": "minio:9000",
            "ACCESS_KEY": "datalake",
            "SECRET_KEY": "datalake",
        }
        return config
    else:
        # Pulling from local .env or hardcoded defaults for testing
        from dotenv import dotenv_values

        return dotenv_values("gtfs/transform/.env")
