import duckdb
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def save_positions_to_storage(config, positions_table):
    """
    Trusted Layer:
    Saves a list of position tuples to MinIO in Parquet format.
    - Partitioning: year/month/day/hour (Hive style)
    - Filename: positions_HHMM.parquet (based on extraction_ts)
    - Prevents overwriting within the same hour by using unique minute-based names.
    """
    if not positions_table:
        logger.warning("No positions to save. Table is empty.")
        return

    # 1. Define schema (Must match the order of your list of tuples)
    columns = [
        "extracao_ts",
        "veiculo_id",
        "linha_lt",
        "linha_code",
        "linha_sentido",
        "lt_destino",
        "lt_origem",
        "veiculo_prefixo",
        "veiculo_acessivel",
        "veiculo_ts",
        "veiculo_lat",
        "veiculo_long",
        "is_circular",
        "first_stop_id",
        "first_stop_lat",
        "first_stop_lon",
        "last_stop_id",
        "last_stop_lat",
        "last_stop_lon",
        "distance_to_first_stop",
        "distance_to_last_stop",
    ]

    try:
        # 2. Convert to DataFrame and prepare time metadata
        df = pd.DataFrame(positions_table, columns=columns)

        # Ensure extracao_ts is datetime to extract components
        df["extracao_ts"] = pd.to_datetime(df["extracao_ts"])

        # Determine the filename based on the actual extraction time (HHMM)
        # Assuming one extraction per run, we take the timestamp of the first row
        batch_ts = df["extracao_ts"].iloc[0]
        file_name = batch_ts.strftime("positions_%H%M.parquet")

        # Create Partition Strings (Zero-padded for correct sorting)
        df["year"] = df["extracao_ts"].dt.strftime("%Y")
        df["month"] = df["extracao_ts"].dt.strftime("%m")
        df["day"] = df["extracao_ts"].dt.strftime("%d")
        df["hour"] = df["extracao_ts"].dt.strftime("%H")

        # 3. Initialize DuckDB for the S3 transfer
        con = duckdb.connect(":memory:")

        # Setup MinIO credentials and S3 settings
        con.execute(f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_endpoint='{config["MINIO_ENDPOINT"]}'; 
            SET s3_access_key_id='{config["ACCESS_KEY"]}';
            SET s3_secret_access_key='{config["SECRET_KEY"]}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)

        # 4. Export to MinIO
        # The base path is the folder; PARTITION_BY adds the year/month/day/hour folders;
        # file_name ensures we don't overwrite previous runs in the same hour.
        output_base_path = (
            f"s3://{config['TRUSTED_BUCKET']}/{config['APP_FOLDER']}/positions"
        )

        logger.info(
            f"Exporting {len(df)} rows to {output_base_path} partitioned by hour..."
        )

        # con.execute(f"""
        #     COPY (SELECT * FROM df)
        #     TO '{output_base_path}/{file_name}'
        #     (FORMAT PARQUET, PARTITION_BY (year, month, day, hour), OVERWRITE_OR_IGNORE 1);
        # """)

        # The base path should only go up to 'positions'
        output_base_path = (
            f"s3://{config['TRUSTED_BUCKET']}/{config['APP_FOLDER']}/positions"
        )

        # We use {filename} as a template so DuckDB knows exactly what to call the file
        # inside the partition folders.
        con.execute(f"""
            COPY (SELECT * FROM df) 
            TO '{output_base_path}' 
            (
                FORMAT PARQUET, 
                PARTITION_BY (year, month, day, hour), 
                FILENAME_PATTERN 'positions_{batch_ts.strftime("%H%M")}_',
                OVERWRITE_OR_IGNORE 1
            );
        """)

        logger.info(f"Successfully saved {file_name} to Trusted Layer.")

    except Exception as e:
        logger.error(f"Failed to save positions to Trusted Layer: {e}")
        raise
    finally:
        if "con" in locals():
            con.close()
