from src.infra.minio_functions import write_generic_bytes_to_minio
from datetime import datetime
from zoneinfo import ZoneInfo
import json
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_data_to_raw(config, data, hour_minute):
    def get_config(config):
        raw_bucket_name = config["SOURCE_BUCKET"]
        app_folder = config["APP_FOLDER"]
        connection_data = {
            "minio_endpoint": config["MINIO_ENDPOINT"],
            "access_key": config["ACCESS_KEY"],
            "secret_key": config["SECRET_KEY"],
            "secure": False,
        }
        return raw_bucket_name, app_folder, connection_data

    raw_bucket_name, app_folder, connection_data = get_config(config)
    iso_timestamp_str = json.loads(data).get("metadata").get("extracted_at")
    dt_utc = datetime.fromisoformat(iso_timestamp_str)
    dt_object = dt_utc.astimezone(ZoneInfo("America/Sao_Paulo"))
    year = dt_object.year
    month = f"{dt_object.month:02d}"
    day = f"{dt_object.day:02d}"
    if data:
        prefix = f"{app_folder}/year={year}/month={month}/day={day}/"
        base_file_name = "posicoes_onibus"
        destination_object_name = (
            f"{prefix}{base_file_name}-{year}{month}{day}{hour_minute}.json"
        )
        try:
            write_generic_bytes_to_minio(
                connection_data,
                buffer=data.encode("utf-8"),
                bucket_name=raw_bucket_name,
                object_name=destination_object_name,
            )
        except Exception as e:
            logger.error(
                "Error writing data to MinIO bucket "
                f"'{raw_bucket_name}' with object name '{destination_object_name}'."
            )
            logger.error(f"Exception details: {e}")
    else:
        logger.error("No records found to write to the destination bucket.")
