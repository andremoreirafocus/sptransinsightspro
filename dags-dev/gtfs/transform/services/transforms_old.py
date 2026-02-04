from infra.db import save_table_to_db
from gtfs.transform.services.load_raw_csv import load_raw_csv
from infra.get_pandas_buffer_from_csv_buffer import (
    get_pandas_buffer_from_csv_buffer,
)

# This logger inherits the configuration from the root logger in main.py
# logger = logging.getLogger(__name__)


def transform_routes_old(config):
    table_name = "routes"
    print(f"Transforming {table_name}...")
    csv_bytes = load_raw_csv(config, table_name)
    buffer, columns = get_pandas_buffer_from_csv_buffer(csv_bytes)
    save_table_to_db(config, table_name, columns, buffer)
    print("Transformation successful.")


def transform_trips_old(config):
    table_name = "trips"
    print(f"Transforming {table_name}...")
    csv_bytes = load_raw_csv(config, table_name)
    buffer, columns = get_pandas_buffer_from_csv_buffer(csv_bytes)
    save_table_to_db(config, table_name, columns, buffer)
    print("Transformation successful.")


def transform_stop_times_old(config):
    table_name = "stop_times"
    print(f"Transforming {table_name}...")
    csv_bytes = load_raw_csv(config, table_name)
    buffer, columns = get_pandas_buffer_from_csv_buffer(csv_bytes)
    save_table_to_db(config, table_name, columns, buffer)
    print("Transformation successful.")


def transform_stops_old(config):
    table_name = "stops"
    print(f"Transforming {table_name}...")
    csv_bytes = load_raw_csv(config, table_name)
    buffer, columns = get_pandas_buffer_from_csv_buffer(csv_bytes)
    save_table_to_db(config, table_name, columns, buffer)
    print("Transformation successful.")


def transform_calendar_old(config):
    table_name = "calendar"
    print(f"Transforming {table_name}...")
    csv_bytes = load_raw_csv(config, table_name)
    buffer, columns = get_pandas_buffer_from_csv_buffer(csv_bytes)
    save_table_to_db(config, table_name, columns, buffer)
    print("Transformation successful.")


def transform_frequencies_old(config):
    table_name = "frequencies"
    print(f"Transforming {table_name}...")
    csv_bytes = load_raw_csv(config, table_name)
    buffer, columns = get_pandas_buffer_from_csv_buffer(csv_bytes)
    save_table_to_db(config, table_name, columns, buffer)
    print("Transformation successful.")
