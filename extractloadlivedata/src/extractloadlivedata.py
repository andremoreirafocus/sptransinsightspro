from src.services.extract_buses_positions import (
    extract_buses_positions_with_retries,
    get_buses_positions_with_metadata,
)
from src.services.save_bus_positions import (
    # save_bus_positions_to_storage,
    save_bus_positions_to_local_volume,
    save_bus_positions_to_storage_with_retries,
)
from src.config import get_config


def extractloadlivedata():
    config = get_config()
    buses_positions_payload = extract_buses_positions_with_retries(config)
    download_successful = buses_positions_payload is not None
    if download_successful:
        buses_positions, _ = get_buses_positions_with_metadata(buses_positions_payload)
        save_bus_positions_to_local_volume(config, buses_positions)
        save_bus_positions_to_storage_with_retries(config, buses_positions)
