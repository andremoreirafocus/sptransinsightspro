import duckdb
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def save_positions_to_storage(config, positions_table):
    """
    Saves the positions list to MinIO in Parquet format,
    partitioned by year, month, and day.
    """
    if not positions_table:
        logger.warning("No positions to save.")
        return

    # Define column names (must match your list of tuples order)
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
        # 1. Convert list of tuples to DataFrame
        df = pd.DataFrame(positions_table, columns=columns)

        # 2. Extract partition columns from the timestamp
        # Ensure extracao_ts is a datetime object
        df["extracao_ts"] = pd.to_datetime(df["extracao_ts"])
        df["year"] = df["extracao_ts"].dt.strftime("%Y")
        df["month"] = df["extracao_ts"].dt.strftime("%m")
        df["day"] = df["extracao_ts"].dt.strftime("%d")

        # 3. Connect to DuckDB
        con = duckdb.connect(":memory:")

        # 4. Configure MinIO
        con.execute(f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_endpoint='{config["MINIO_ENDPOINT"]}'; 
            SET s3_access_key_id='{config["ACCESS_KEY"]}';
            SET s3_secret_access_key='{config["SECRET_KEY"]}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)

        # 5. Export to MinIO with Hive Partitioning
        output_path = (
            f"s3://{config['TRUSTED_BUCKET']}/{config['APP_FOLDER']}/positions"
        )

        logger.info(
            f"Exporting {len(df)} rows to partitioned parquet at {output_path}..."
        )

        # DuckDB can query the Pandas 'df' directly in the same session
        con.execute(f"""
            COPY (SELECT * FROM df) 
            TO '{output_path}' 
            (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE 1);
        """)

        logger.info("Successfully saved partitioned Parquet to MinIO.")

    except Exception as e:
        logger.error(f"Error saving positions to MinIO: {e}")
        raise
    finally:
        if "con" in locals():
            con.close()
