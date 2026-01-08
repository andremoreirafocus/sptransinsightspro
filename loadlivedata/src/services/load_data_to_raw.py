from src.infra.get_minio_connection_data import get_minio_connection_data
from src.infra.minio_functions import write_generic_bytes_to_minio
from datetime import datetime
import json


def load_data_to_raw(data, raw_bucket_name, app_folder, hour_minute):
    iso_timestamp_str = json.loads(data).get("metadata").get("extracted_at")
    print(type(iso_timestamp_str))
    dt_object = datetime.fromisoformat(iso_timestamp_str)
    print(f"Data extracted at: {dt_object}")
    print(f"Loading data to raw bucket from {iso_timestamp_str}...")
    # year = datetime.now().strftime("%Y")
    year = dt_object.year
    month = f"{dt_object.month:02d}"
    day = f"{dt_object.day:02d}"
    print(f"Year: {year}, Month: {month}, Day: {day}")
    if data:
        prefix = f"{app_folder}/year={year}/month={month}/day={day}/"
        base_file_name = "posicoes_onibus"
        destination_object_name = (
            f"{prefix}{base_file_name}-{year}{month}{day}{hour_minute}.json"
        )
        connection_data = get_minio_connection_data()
        write_generic_bytes_to_minio(
            connection_data,
            buffer=data.encode("utf-8"),
            bucket_name=raw_bucket_name,
            object_name=destination_object_name,
        )
    else:
        print("No records found to write to the destination bucket.")
