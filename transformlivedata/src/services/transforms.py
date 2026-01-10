from src.infra.db import db_cursor
from src.infra.minio_functions import read_file_from_minio
from src.infra.get_minio_connection_data import get_minio_connection_data
import logging
import json
from dateutil import parser

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_positions(source_bucket, app_folder):
    """
    Load position data from source bucket and app folder.
    :param source_bucket: Source bucket name
    :param app_folder: Application folder path
    :return: Loaded data
    """
    logger.info(
        f"Loading position data from bucket: {source_bucket}, folder: {app_folder}"
    )
    # Add your logic to load position data here
    # Example: read files from MinIO, parse them, and return as a list of records
    year = 2026
    month = "01"
    day = "10"
    prefix = f"{app_folder}/year={year}/month={month}/day={day}/"
    hour_minute = "0842"
    base_file_name = "posicoes_onibus"
    connection_data = get_minio_connection_data()
    object_name = f"{prefix}{base_file_name}-{year}{month}{day}{hour_minute}.json"
    datastr = read_file_from_minio(connection_data, source_bucket, object_name)
    logger.info(f"Loaded {len(datastr)} bytes from {object_name}")
    # print(data)
    data = json.loads(datastr)
    return data


def data_structure_is_valid(data):
    """
    Validate the structure of the incoming data.
    :param data: The data to validate
    :return: True if valid, False otherwise
    """
    if not isinstance(data, dict):
        logger.error("Data does not have a valid structure.")
        return False
    required_fields = ["payload", "metadata"]
    for field in required_fields:
        if field not in data:
            logger.error(f"Missing required field: {field}")
            logger.error(f"Data content: {data}")
            return False
    if not isinstance(data.get("metadata"), dict):
        logger.error("Data metadata does not have a valid structure.")
        return False
    required_fields = ["source", "extracted_at", "total_vehicles"]
    for field in required_fields:
        if field not in data.get("metadata"):
            logger.error(f"Missing required metadata field: {field}")
            logger.error(f"Metadata content: {data.get('metadata')}")
            return False
    if not isinstance(data.get("payload"), dict):
        logger.error("Data payload does not have a valid structure.")
        logger.error(f"Payload content: {data.get('payload')}")
        logger.error(f"Metadata content: {data.get('metadata')}")
        return False
    required_fields = ["hr", "l"]
    for field in required_fields:
        if field not in data.get("payload"):
            logger.error(f"Missing required payload field: {field}")
            return False
    return True


def get_positions_table_from_raw(raw_positions):
    positions_table = []
    if not data_structure_is_valid(raw_positions):
        logger.error("Raw positions data structure is invalid.")
        return None
    payload = raw_positions.get("payload")
    metadata = raw_positions.get("metadata")
    if "hr" not in payload:
        logger.error("No 'hr' field found in raw positions data.")
        return None
    if "l" not in payload:
        logger.error("No 'l' field found in raw positions data.")
        return None
    for line in payload["l"]:
        # print(line)
        number_of_vehicles = 0
        for vehicle in line.get("vs", []):
            # print(vehicle)
            # vehicle_record = {
            #     "veiculo_id": vehicle.get("p"),
            #     # "hora": payload.get("hr"),
            #     "timestamp_extracao": metadata.get("extracted_at"),
            #     "linha_lt": line.get("c"),
            #     "linha_code": line.get("cl"),
            #     "linha_sentido": line.get("sl"),
            #     "lt_destino": line.get("lt0"),
            #     "lt_origem": line.get("lt1"),
            #     "veiculo_prefixo": vehicle.get("p"),
            #     "veiculo_acessivel": vehicle.get("a"),
            #     "veiculo_ts": vehicle.get("ta"),
            #     "veiculo_lat": vehicle.get("py"),
            #     "veiculo_long": vehicle.get("px"),
            # }
            vehicle_record = {
                "veiculo_id": int(vehicle.get("p")),
                # "hora": payload.get("hr"),
                "timestamp_extracao": metadata.get("extracted_at"),
                "linha_lt": line.get("c"),
                "linha_code": int(line.get("cl")),
                "linha_sentido": int(line.get("sl")),
                "lt_destino": line.get("lt0"),
                "lt_origem": line.get("lt1"),
                "veiculo_prefixo": int(vehicle.get("p")),
                "veiculo_acessivel": bool(vehicle.get("a")),
                # "veiculo_ts": vehicle.get("ta"),
                "veiculo_ts": parser.parse(vehicle.get("ta")),
                "veiculo_lat": float(vehicle.get("py")),
                "veiculo_long": float(vehicle.get("px")),
            }

            print(vehicle_record)
            number_of_vehicles += 1
            positions_table.append(vehicle_record)
        if number_of_vehicles != int(line.get("qv")):
            logger.warning(
                f"Expected {line.get('q', 0)} vehicles for line {line.get('qv')}, but found {number_of_vehicles}."
            )
        else:
            logger.info(
                f"Processed {number_of_vehicles} vehicles for line {line.get('qv')}."
            )
    return positions_table


def transform_position(source_bucket, app_folder, table_name):
    logger.info("Transforming position...")
    raw_positions = load_positions(source_bucket, app_folder)
    if not raw_positions:
        logger.error("No position data found to transform.")
        return
    positions_table = get_positions_table_from_raw(raw_positions)
    if not positions_table:
        logger.error("No valid position records found after transformation.")
    return
    TRANSFORMATION_SQL = """
    INSERT INTO trusted.my_fact_table (col1, col2, col3)
    SELECT
        col1,
        col2,
        col3
    FROM staging.my_raw_table
    WHERE ingestion_date = %(ingestion_date)s;
    """
    ingestion_date = "2024-01-01"  # Example ingestion date
    params = {"ingestion_date": ingestion_date}
    with db_cursor() as cur:
        cur.execute(TRANSFORMATION_SQL, params)  # execute arbitrary SQL via psycopg2.

    print("Position transformed successfully.")
