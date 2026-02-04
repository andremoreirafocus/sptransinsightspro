from gtfs.transform.services.load_raw_csv import load_raw_csv
from infra.get_parquet_buffer_from_csv_buffer import (
    get_parquet_buffer_from_csv_buffer,
)
from gtfs.transform.services.save_buffer_to_storage import save_buffer_to_storage
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def transform_csv_table(config, table_name):
    print(f"Transforming {table_name}...")
    csv_bytes = load_raw_csv(config, table_name)
    parquet_buffer = get_parquet_buffer_from_csv_buffer(csv_bytes)
    file_name = f"{table_name}.parquet"
    save_buffer_to_storage(config, file_name, parquet_buffer)
    print("Transformation successful.")


def transform_routes(config):
    table_name = "routes"
    transform_csv_table(config, table_name)


def transform_trips(config):
    table_name = "trips"
    transform_csv_table(config, table_name)


def transform_stop_times(config):
    table_name = "stop_times"
    transform_csv_table(config, table_name)


def transform_stops(config):
    table_name = "stops"
    transform_csv_table(config, table_name)


def transform_calendar(config):
    table_name = "calendar"
    transform_csv_table(config, table_name)


def transform_frequencies(config):
    table_name = "frequencies"
    transform_csv_table(config, table_name)
