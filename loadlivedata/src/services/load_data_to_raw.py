from src.infra.get_minio_connection_data import get_minio_connection_data
from src.infra.minio_functions import write_generic_bytes_to_minio
from datetime import datetime


def load_data_to_raw(data, raw_bucket_name, app_folder, hour_minute):
    year = datetime.now().strftime("%Y")
    month = datetime.now().strftime("%m")
    day = datetime.now().strftime("%d")

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
