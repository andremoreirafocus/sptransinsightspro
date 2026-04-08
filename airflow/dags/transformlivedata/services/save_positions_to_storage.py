import pandas as pd
import logging
from typing import Any, Dict, Optional, Tuple
from infra.duck_db_v2 import get_duckdb_connection

logger = logging.getLogger(__name__)



def save_positions_to_storage(
    config: Dict[str, Any],
    positions_df: Optional[pd.DataFrame],
    target_bucket: str,
) -> None:
    """
    Storage Layer:
    Saves a list of position tuples to MinIO in Parquet format.
    - Partitioning: year/month/day/hour (Hive style)
    - Filename pattern: positions_HHMM_*
    - Supports target_bucket: "trusted" or "quarantined"
    """

    def get_config(
        config: Dict[str, Any], target_bucket: str
    ) -> Tuple[str, str, str, Dict[str, Any]]:
        try:
            storage = config["storage"]
            tables = config["tables"]
            if target_bucket == "trusted":
                bucket_name = storage["trusted_bucket"]
            elif target_bucket == "quarantined":
                bucket_name = storage["quarantined_bucket"]
            else:
                raise ValueError(
                    f"Invalid target_bucket '{target_bucket}'. Use 'trusted' or 'quarantined'."
                )
            app_folder = storage["app_folder"]
            positions_table_name = tables["positions_table_name"]
            connection_data = {
                "minio_endpoint": storage["minio_endpoint"],
                "access_key": storage["access_key"],
                "secret_key": storage["secret_key"],
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
        con = get_duckdb_connection(connection_data)
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
