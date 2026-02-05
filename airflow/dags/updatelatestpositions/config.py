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
            "MINIO_ENDPOINT": "minio:9000",
            "ACCESS_KEY": "datalake",
            "SECRET_KEY": "datalake",
            "LATEST_POSITIONS_TABLE_NAME": "refined.latest_positions",
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

        return dotenv_values("updatelatestpositions/.env")
