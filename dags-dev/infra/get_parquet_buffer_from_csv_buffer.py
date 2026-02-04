from io import BytesIO
import pandas as pd
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def get_parquet_buffer_from_csv_buffer(csv_buffer):
    df = pd.read_csv(csv_buffer)
    print(df.head())
    print(df.dtypes)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy")
    buffer.seek(0)
    return buffer
