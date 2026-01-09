import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def transform_position(source_bucket, app_folder, db_connection, table_name):
    print("Transforming position...")
    # Add your logic to transform position here
    print("Position transformed successfully.")
