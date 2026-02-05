import duckdb
import pandas as pd
import logging
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def create_latest_positions_table(config):
    """
    1. Uses DuckDB to extract the latest snapshot from MinIO.
    2. Returns the result as a Pandas DataFrame.
    3. Overwrites the 'latest_positions' table in Postgres.
    """
    # 1. Setup paths and credentials
    bucket_name = config["TRUSTED_BUCKET"]
    app_folder = config["APP_FOLDER"]
    positions_s3_path = f"s3://{bucket_name}/{app_folder}/positions/*/*/*/*.parquet"
    target_table = config["LATEST_POSITIONS_TABLE_NAME"]

    # 2. DuckDB Processing (The Heavy Lifting)
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_endpoint='{config["MINIO_ENDPOINT"]}'; 
            SET s3_access_key_id='{config["ACCESS_KEY"]}';
            SET s3_secret_access_key='{config["SECRET_KEY"]}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)

        # We execute the query directly into a DataFrame
        # No 'CREATE TABLE' here - we just want the result set
        logger.info("Executing analytical query in DuckDB over MinIO...")

        refined_df = con.execute(f"""
            WITH latest_snapshot AS (
                SELECT MAX(extracao_ts) AS max_ts
                FROM read_parquet('{positions_s3_path}')
            )
            SELECT 
                p.veiculo_ts,
                p.veiculo_id, 
                p.veiculo_lat,  
                p.veiculo_long, 
                p.linha_lt, 
                p.linha_sentido,
                p.linha_lt || '-' || (
                    CASE 
                        WHEN p.linha_sentido = 1 THEN '0' 
                        WHEN p.linha_sentido = 2 THEN '1' 
                        ELSE NULL 
                    END
                ) AS trip_id
            FROM read_parquet('{positions_s3_path}') p
            JOIN latest_snapshot ls ON p.extracao_ts = ls.max_ts;
        """).df()

        if refined_df.empty:
            logger.warning("Query returned 0 rows. Skipping Postgres update.")
            return

        # 3. Save result to Postgres for PowerBI (Low Latency Layer)
        logger.info(f"Transferring {len(refined_df)} rows to Postgres...")

        db_uri = f"postgresql://{config['DB_USER']}:{config['DB_PASSWORD']}@{config['DB_HOST']}:{config['DB_PORT']}/{config['DB_DATABASE']}"
        engine = create_engine(db_uri)

        # 'if_exists=replace' effectively Drops and Recreates the table in one go
        # This ensures PowerBI always sees a clean, current snapshot
        refined_df.to_sql(
            name=target_table,
            con=engine,
            schema="refined",  # Change to your actual schema name
            if_exists="replace",
            index=False,
            method="multi",  # Speeds up the insert
            chunksize=1000,  # Prevents memory spikes during transfer
        )

        logger.info(f"Successfully updated {target_table} in Postgres.")

    except Exception as e:
        logger.error(f"Failed to update latest positions: {e}")
        raise
    finally:
        con.close()
