from refinedfinishedtrips.services.extract_trips_from_positions import (
    extract_raw_trips_metadata,
    filter_healthy_trips,
    generate_trips_table,
)
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def extract_trips_per_line_per_vehicle_pandas(linha_lt, veiculo_id, df_preloaded=None):
    try:
        if df_preloaded is None:
            return
        position_records = df_preloaded.to_dict("records")
        if not position_records:
            logger.debug(f"No positions for line {linha_lt} vehicle {veiculo_id}")
            return
        raw_trips_metadata = extract_raw_trips_metadata(position_records)
        if not raw_trips_metadata:
            logger.debug(f"No trips for line {linha_lt} vehicle {veiculo_id}")
            return
        clean_trips_metadata = filter_healthy_trips(
            raw_trips_metadata, position_records
        )
        if not clean_trips_metadata:
            logger.debug(f"No clean trips for line {linha_lt} vehicle {veiculo_id}")
            return
        finished_trips = generate_trips_table(
            position_records, clean_trips_metadata, linha_lt, veiculo_id
        )
        return finished_trips
    except Exception as e:
        logger.error(f"Error processing {linha_lt}/{veiculo_id}: {e}")
        print(f"Error processing {linha_lt}/{veiculo_id}: {e}")
        raise TypeError(f"Error processing {linha_lt}/{veiculo_id}:")
