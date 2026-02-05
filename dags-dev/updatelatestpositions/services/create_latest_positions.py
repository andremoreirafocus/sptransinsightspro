import duckdb
import pandas as pd
import logging
from datetime import datetime, timedelta
from minio import Minio
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def create_latest_positions_table(config):
    """
    Refined Layer (Cost-Optimized):
    - No recursive listing.
    - Explicitly checks the current and previous hour prefixes.
    - If the pipeline was delayed, it will find the most recent processed data.
    """
    bucket = config["TRUSTED_BUCKET"]
    target_table = config["LATEST_POSITIONS_TABLE_NAME"]

    try:
        client = Minio(
            config["MINIO_ENDPOINT"].replace("http://", "").replace("https://", ""),
            access_key=config["ACCESS_KEY"],
            secret_key=config["SECRET_KEY"],
            secure=False,
        )

        # 1. SURGICAL PRUNING: Check only the most likely hours
        # We start with the current hour and look back up to 6 hours (configurable)
        # to find the latest available batch without a full scan.
        latest_file_path = None
        now = datetime.now()

        for i in range(6):  # Look back window
            check_time = now - timedelta(hours=i)
            # Match your Hive structure
            prefix = f"{config['APP_FOLDER']}/positions/{check_time.strftime('year=%Y/month=%m/day=%d/hour=%H')}/"

            # Non-recursive list of just this specific prefix
            objects = client.list_objects(bucket, prefix=prefix, recursive=True)
            # Get the highest filename in this specific hour
            found_files = sorted(
                [
                    obj.object_name
                    for obj in objects
                    if obj.object_name.endswith(".parquet")
                ]
            )

            if found_files:
                latest_file_path = f"s3://{bucket}/{found_files[-1]}"
                break

        if not latest_file_path:
            logger.error(
                "No recent data found in the last 6 hours. Cost-saving scan aborted."
            )
            return

        logger.info(f"Surgical discovery successful: {latest_file_path}")

        # 2. DUCKDB: Direct read of the single identified file
        con = duckdb.connect(":memory:")
        con.execute(f"""
            INSTALL httpfs; LOAD httpfs;
            SET s3_endpoint='{config["MINIO_ENDPOINT"]}'; 
            SET s3_access_key_id='{config["ACCESS_KEY"]}';
            SET s3_secret_access_key='{config["SECRET_KEY"]}';
            SET s3_use_ssl=false; SET s3_url_style='path';
        """)

        refined_df = con.execute(f"""
            SELECT veiculo_ts, veiculo_id, veiculo_lat, veiculo_long, linha_lt, linha_sentido,
                   linha_lt || '-' || (CASE WHEN linha_sentido = 1 THEN '0' 
                                            WHEN linha_sentido = 2 THEN '1' ELSE NULL END) AS trip_id
            FROM read_parquet('{latest_file_path}')
        """).df()

        # 3. POSTGRES: Load
        db_uri = f"postgresql://{config['DB_USER']}:{config['DB_PASSWORD']}@{config['DB_HOST']}:{config['DB_PORT']}/{config['DB_DATABASE']}"
        engine = create_engine(db_uri)
        refined_df.to_sql(
            name=target_table,
            con=engine,
            schema="refined",
            if_exists="replace",
            index=False,
        )

    except Exception as e:
        logger.error(f"Update failed: {e}")
        raise
