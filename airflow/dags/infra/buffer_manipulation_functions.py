from io import BytesIO, StringIO
import pandas as pd
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def convert_buffer_from_csv_to_parquet(csv_buffer):
    df = pd.read_csv(csv_buffer)
    print(df.head())
    print(df.dtypes)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy")
    buffer.seek(0)
    return buffer


def extract_body_and_columns_from_csv_buffer(csv_buffer):
    df = pd.read_csv(csv_buffer)
    print(df.head())
    print(df.dtypes)
    # Example: df = df.astype({"col1": "int64"})  # Enforce types
    buffer = StringIO()
    columns = df.columns.tolist()
    df.to_csv(buffer, index=False, header=False, columns=columns)
    buffer.seek(0)
    return buffer, columns
