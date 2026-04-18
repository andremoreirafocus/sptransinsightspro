from io import BytesIO, StringIO
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def convert_df_to_parquet_buffer(df):
    logger.info(f"Dataframe content:\n{df.head()}")
    logger.info(f"Dataframe dtypes:\n{df.dtypes}")
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy")
    buffer.seek(0)
    return buffer


# def extract_body_and_columns_from_csv_buffer(csv_buffer):
#     df = pd.read_csv(csv_buffer)
#     logger.info(f"Dataframe content:\n{df.head()}")
#     logger.info(f"Dataframe dtypes:\n{df.dtypes}")
#     # Example: df = df.astype({"col1": "int64"})  # Enforce types
#     buffer = StringIO()
#     columns = df.columns.tolist()
#     df.to_csv(buffer, index=False, header=False, columns=columns)
#     buffer.seek(0)
#     return buffer, columns
