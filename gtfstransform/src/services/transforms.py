from src.infra.db import save_routes_to_db
from src.services.load_raw_csv import load_raw_csv
from src.infra.get_pandas_buffer_from_csv_buffer import (
    get_pandas_buffer_from_csv_buffer,
)
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def transform_routes(config):
    SOURCE_BUCKET = config["SOURCE_BUCKET"]
    APP_FOLDER = config["APP_FOLDER"]
    print("Transforming routes...")
    table_name = "routes"
    schema = "trusted"
    csv_bytes = load_raw_csv(SOURCE_BUCKET, APP_FOLDER, table_name)
    buffer, columns = get_pandas_buffer_from_csv_buffer(csv_bytes)
    save_routes_to_db(config, schema, table_name, columns, buffer)

    print("Routes transformed successfully.")


def transform_trips(config):
    print("Transforming trips...")
    # Add your logic to transform trips here
    print("Trips transformed successfully.")


def transform_stop_times(config):
    print("Transforming stop times...")
    # Add your logic to transform stop times here
    print("Stop times transformed successfully.")


def transform_stops(config):
    print("Transforming stops...")
    # Add your logic to transform stops here
    print("Stops transformed successfully.")


def transform_calendar(config):
    print("Transforming calendar...")
    # Add your logic to transform calendar here
    print("Calendar transformed successfully.")


def transform_frequencies(config):
    print("Transforming frequencies...")
    # Add your logic to transform frequencies here
    print("Frequencies transformed successfully.")
