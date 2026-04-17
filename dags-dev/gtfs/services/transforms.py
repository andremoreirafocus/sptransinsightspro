from gtfs.services.load_raw_csv_to_buffer_from_storage import (
    load_raw_csv_to_buffer_from_storage,
)
from infra.buffer_manipulation_functions import (
    convert_df_to_parquet_buffer,
)
from gtfs.services.save_buffer_to_storage import save_buffer_to_storage
import pandas as pd
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def transform_csv_table_to_parquet(
    config,
    table_name,
    load_fn=load_raw_csv_to_buffer_from_storage,
    convert_fn=convert_df_to_parquet_buffer,
    save_fn=save_buffer_to_storage,
):
    logger.info(f"Transforming {table_name}...")
    csv_bytes = load_fn(config, table_name)
    df = pd.read_csv(csv_bytes)
    parquet_buffer = convert_fn(df)
    file_name = f"{table_name}.parquet"
    save_fn(config, file_name, parquet_buffer)
    logger.info("Transformation successful.")


def transform_routes(config):
    table_name = "routes"
    transform_csv_table_to_parquet(config, table_name)


def transform_trips(config):
    table_name = "trips"
    transform_csv_table_to_parquet(config, table_name)


def transform_stop_times(config):
    table_name = "stop_times"
    transform_csv_table_to_parquet(config, table_name)


def transform_stops(config):
    table_name = "stops"
    transform_csv_table_to_parquet(config, table_name)


def transform_calendar(config):
    table_name = "calendar"
    transform_csv_table_to_parquet(config, table_name)


def transform_frequencies(config):
    table_name = "frequencies"
    transform_csv_table_to_parquet(config, table_name)
