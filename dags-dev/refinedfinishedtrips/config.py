import os


def get_config():
    # Check if we are running inside Airflow
    if os.getenv("AIRFLOW_HOME"):
        # Pulling from Airflow Variables
        # from airflow.models import Variable
        config = {
            "ANALYSIS_HOURS_WINDOW": 3,
            "TRUSTED_BUCKET": "trusted",
            "APP_FOLDER": "sptrans",
            "MINIO_ENDPOINT": "minio:9000",
            "ACCESS_KEY": "datalake",
            "SECRET_KEY": "datalake",
            "POSITIONS_TABLE_NAME": "positions",
            "FINISHED_TRIPS_TABLE_NAME": "refined.finished_trips",
            "DB_HOST": "postgres",
            "DB_PORT": 5432,
            "DB_DATABASE": "sptrans_insights",
            "DB_USER": "postgres",
            "DB_PASSWORD": "postgres",
            "DB_SSLMODE": "prefer",
        }
        return config
    else:
        # Pulling from local .env or hardcoded defaults for testing
        from dotenv import dotenv_values

        return dotenv_values("refinedfinishedtrips/.env")
