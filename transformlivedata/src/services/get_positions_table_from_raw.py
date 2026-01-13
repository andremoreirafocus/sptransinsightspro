from dateutil import parser
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def get_positions_table_from_raw(raw_positions):
    def get_record_from_raw(vehicle, line, metadata):
        vehicle_record = (
            parser.parse(metadata.get("extracted_at")),  # extracao_ts
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
