from src.infra.db import get_db_connection
from src.infra.minio_functions import read_file_from_minio
from src.infra.get_minio_connection_data import get_minio_connection_data
import logging
import json
from dateutil import parser
from psycopg2.extras import execute_values


# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


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
    save_positions_to_db(positions_table, table_name)
    logger.info("Positions transformed successfully.")


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
    # logger.info(data)
    data = json.loads(datastr)
    return data


def get_positions_table_from_raw(raw_positions):
    def get_record_from_raw(vehicle, line, metadata):
        vehicle_record = (
            metadata.get("extracted_at"),  # timestamp_extracao
            int(vehicle.get("p")),  # veiculo_id
            line.get("c"),  # linha_lt
            int(line.get("cl")),  # linha_code
            int(line.get("sl")),  # linha_sentido
            line.get("lt0"),  # lt_destino
            line.get("lt1"),  # lt_origem
            int(vehicle.get("p")),  # veiculo_prefixo
            bool(vehicle.get("a")),  # veiculo_acessivel
            parser.parse(vehicle.get("ta")),  # veiculo_ts
            float(vehicle.get("py")),  # veiculo_lat
            float(vehicle.get("px")),  # veiculo_long
        )
        return vehicle_record

    logger.info("Converting raw positions to positions table...")
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
        number_of_vehicles = 0
        for vehicle in line.get("vs", []):
            vehicle_record = get_record_from_raw(vehicle, line, metadata)
            number_of_vehicles += 1
            positions_table.append(vehicle_record)
        if number_of_vehicles != int(line.get("qv")):
            logger.warning(
                f"Expected {line.get('q', 0)} vehicles for line {line.get('qv')}, but found {number_of_vehicles}."
            )
        else:
            if number_of_vehicles % 1000 == 0:
                logger.info(
                    f"Processed {number_of_vehicles} vehicles for line {line.get('qv')}."
                )
        if number_of_vehicles % 1000 != 0:
            logger.info(
                f"Processed {number_of_vehicles} vehicles for line {line.get('qv')}."
            )
    return positions_table


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


def save_positions_to_db(positions_table, table_name):
    """
    Insert 10k+ items from memory list.
    Assumes list format: (timestamp_extracao, veiculo_id, linha_lt, linha_code,
                          linha_sentido, lt_destino, lt_origem, veiculo_prefixo,
                          veiculo_acessivel, veiculo_ts, veiculo_lat, veiculo_long)
    """
    conn = get_db_connection()

    cur = conn.cursor()

    insert_sql = f"""
    INSERT INTO {table_name} (
        timestamp_extracao, veiculo_id, linha_lt, linha_code, linha_sentido,
        lt_destino, lt_origem, veiculo_prefixo, veiculo_acessivel, veiculo_ts,
        veiculo_lat, veiculo_long
    ) VALUES %s
    """

    execute_values(cur, insert_sql, positions_table, page_size=1000)  # Batch internally
    conn.commit()
    cur.close()
    conn.close()
