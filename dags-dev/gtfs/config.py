import os
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


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
            "GTFS_FOLDER": "gtfs",
            "RAW_BUCKET": "raw",
            "TRUSTED_BUCKET": "trusted",
            "TRIP_DETAILS_TABLE_NAME": "trip_details",
            "MINIO_ENDPOINT": "minio:9000",
            "ACCESS_KEY": "datalake",
            "SECRET_KEY": "datalake",
        }
        logger.info("Configuration retrieved for Airflow!")
    else:
        # Pulling from local .env
        from dotenv import dotenv_values

        config = dotenv_values("gtfs/.env")
        logger.info("Configuration retrieved locally!")
    return config
