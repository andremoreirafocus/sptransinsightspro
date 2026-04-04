from infra.sql_db_v2 import bulk_insert_data_table
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def save_trips_to_db(config, trips_table):
    try:
        general = config["general"]
        tables = general["tables"]
        database = general["database"]
        table_name = tables["finished_trips_table_name"]
        connection = {
            "host": database["host"],
            "port": database["port"],
            "database": database["database"],
            "user": database["user"],
            "password": database["password"],
        }
    except KeyError as e:
        logger.error(f"Missing required configuration key: {e}")
        raise
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
    bulk_insert_data_table(connection, insert_sql, trips_table)
