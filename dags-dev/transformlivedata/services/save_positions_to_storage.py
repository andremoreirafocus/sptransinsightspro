import duckdb
import pandas as pd
import logging

logger = logging.getLogger(__name__)



def save_positions_to_storage(config, positions_df, target_bucket):
    """
    Storage Layer:
    Saves a list of position tuples to MinIO in Parquet format.
    - Partitioning: year/month/day/hour (Hive style)
    - Filename pattern: positions_HHMM_*
    - Supports target_bucket: "trusted" or "quarantined"
    """

    def get_config(config, target_bucket):
        try:
            if target_bucket == "trusted":
                bucket_name = config["TRUSTED_BUCKET"]
            elif target_bucket == "quarantined":
                bucket_name = config["QUARANTINED_BUCKET"]
            else:
                raise ValueError(
                    f"Invalid target_bucket '{target_bucket}'. Use 'trusted' or 'quarantined'."
                )
            app_folder = config["APP_FOLDER"]
            positions_table_name = config["POSITIONS_TABLE_NAME"]
            connection_data = {
                "minio_endpoint": config["MINIO_ENDPOINT"],
                "access_key": config["ACCESS_KEY"],
                "secret_key": config["SECRET_KEY"],
                "secure": False,
            }
            return bucket_name, app_folder, positions_table_name, connection_data
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise ValueError(f"Missing required configuration key: {e}")

    bucket_name, app_folder, positions_table_name, connection_data = get_config(
        config, target_bucket
    )
    if positions_df is None or positions_df.empty:
        logger.warning("No positions to save. DataFrame is empty.")
        return
    try:
        positions_df["extracao_ts"] = pd.to_datetime(positions_df["extracao_ts"])
        batch_ts = positions_df["extracao_ts"].iloc[0]
        file_name = batch_ts.strftime("positions_%H%M.parquet")
        positions_df["year"] = positions_df["extracao_ts"].dt.strftime("%Y")
        positions_df["month"] = positions_df["extracao_ts"].dt.strftime("%m")
        positions_df["day"] = positions_df["extracao_ts"].dt.strftime("%d")
        positions_df["hour"] = positions_df["extracao_ts"].dt.strftime("%H")
        con = duckdb.connect(":memory:")
        con.execute(f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_endpoint='{connection_data["minio_endpoint"]}'; 
            SET s3_access_key_id='{connection_data["access_key"]}';
            SET s3_secret_access_key='{connection_data["secret_key"]}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)
        output_base_path = f"s3://{bucket_name}/{app_folder}/{positions_table_name}"
        logger.info(
            f"Exporting {len(positions_df)} rows to {output_base_path} partitioned by hour..."
        )
        con.execute(f"""
            COPY (SELECT * FROM positions_df) 
            TO '{output_base_path}' 
            (
                FORMAT PARQUET, 
                PARTITION_BY (year, month, day, hour), 
                FILENAME_PATTERN 'positions_{batch_ts.strftime("%H%M")}_',
                OVERWRITE_OR_IGNORE 1
            );
        """)
        logger.info(f"Successfully saved {file_name} to {target_bucket} layer.")
    except Exception as e:
        logger.error(f"Failed to save positions to {target_bucket} layer: {e}")
        raise ValueError(f"Failed to save positions to {target_bucket} layer: {e}")
    finally:
        if "con" in locals():
            con.close()
            logger.info("DuckDB connection closed.")
