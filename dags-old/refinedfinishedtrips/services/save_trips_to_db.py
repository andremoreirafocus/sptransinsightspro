from refinedfinishedtrips.infra.db import bulk_insert_data_table
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def save_trips_to_db(config, trips_table):
    table_name = config["FINISHED_TRIPS_TABLE_NAME"]
    insert_sql = f"""
    INSERT INTO {table_name} (
            trip_id,
            vehicle_id,
            trip_start_time,
            trip_end_time,
            duration,
            is_circular,
            average_speed
    ) VALUES %s
    """
    bulk_insert_data_table(config, insert_sql, trips_table)
