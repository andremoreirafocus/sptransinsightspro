import os


def get_config():
    # Check if we are running inside Airflow
    if os.getenv("AIRFLOW_HOME"):
        # Pulling from Airflow Variables
        # from airflow.models import Variable
        config = {
            "GTFS_URL": "http://www.sptrans.com.br/umbraco/Surface/PerfilDesenvolvedor/BaixarGTFS",
            "LOGIN": "andre.moreira.engineer",
            "PASSWORD": "T3st3@p1",
            "LOCAL_DOWNLOADS_FOLDER": "gtfs_files",
            "APP_FOLDER": "sptrans",
            "RAW_BUCKET": "raw",
            "TRUSTED_BUCKET": "trusted",
            "MINIO_ENDPOINT": "minio:9000",
            "ACCESS_KEY": "datalake",
            "SECRET_KEY": "datalake",
        }
        return config
    else:
        # Pulling from local .env or hardcoded defaults for testing
        from dotenv import dotenv_values

    return dotenv_values("gtfs/.env")
