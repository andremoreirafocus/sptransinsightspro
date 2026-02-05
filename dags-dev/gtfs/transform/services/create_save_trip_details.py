import duckdb
import logging

logger = logging.getLogger(__name__)


def create_trip_details_table(config):
    def get_config(config):
        # Using .get() with fallback or checking existence to avoid KeyErrors
        try:
            bucket_name = config["TRUSTED_BUCKET"]
            app_folder = config["APP_FOLDER"]
            connection_data = {
                "minio_endpoint": config["MINIO_ENDPOINT"],
                "access_key": config["ACCESS_KEY"],
                "secret_key": config["SECRET_KEY"],
                "secure": False,
            }
            return bucket_name, app_folder, connection_data
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    # Initialize connection variable for the finally block
    con = None

    try:
        bucket_name, app_folder, connection_data = get_config(config)

        logger.info("Connecting to DuckDB...")
        con = duckdb.connect("gtfs.db")

        # 2. Setup MinIO Credentials
        # We wrap this in a block because network/credential issues are common here
        con.execute(f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_endpoint='{connection_data["minio_endpoint"]}'; 
            SET s3_access_key_id='{connection_data["access_key"]}';
            SET s3_secret_access_key='{connection_data["secret_key"]}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)

        # 3. Define views
        con.execute("CREATE SCHEMA IF NOT EXISTS trusted;")

        stop_times_table_path = f"{bucket_name}/{app_folder}/stop_times"
        stops_table_path = f"{bucket_name}/{app_folder}/stops"

        con.execute(f"""
            CREATE OR REPLACE VIEW trusted.stop_times AS 
            SELECT * FROM read_parquet('s3://{stop_times_table_path}/*.parquet');
        """)
        con.execute(f"""
            CREATE OR REPLACE VIEW trusted.stops AS 
            SELECT * FROM read_parquet('s3://{stops_table_path}/*.parquet');
        """)

        # 4. CTAS Query (Changed to OR REPLACE to allow re-runs)
        sql_command = """
        CREATE OR REPLACE TABLE trusted.trip_details AS
        WITH trip_extremes AS (
            SELECT 
                trip_id,
                MAX(CASE WHEN start_rank = 1 THEN stop_id END) AS first_stop_id,
                MAX(CASE WHEN end_rank = 1 THEN stop_id END) AS last_stop_id
            FROM (
                SELECT 
                    trip_id, stop_id, 
                    ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY stop_sequence ASC) as start_rank,
                    ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY stop_sequence DESC) as end_rank
                FROM trusted.stop_times
            ) ranked_data
            GROUP BY trip_id
        ),
        trip_metrics AS (
            SELECT 
                te.trip_id, te.first_stop_id, s1.stop_name AS first_stop_name,
                s1.stop_lat AS first_stop_lat, s1.stop_lon AS first_stop_lon,
                te.last_stop_id, s2.stop_name AS last_stop_name,
                s2.stop_lat AS last_stop_lat, s2.stop_lon AS last_stop_lon,
                ROUND(SQRT(POW(s2.stop_lat - s1.stop_lat, 2) + POW(s2.stop_lon - s1.stop_lon, 2)) * 106428) AS trip_linear_distance
            FROM trip_extremes te
            JOIN trusted.stops s1 ON te.first_stop_id = s1.stop_id
            JOIN trusted.stops s2 ON te.last_stop_id = s2.stop_id
        )
        SELECT *, (trip_linear_distance = 0) AS is_circular FROM trip_metrics;
        """

        logger.info("Executing CTAS for trip_details...")
        con.execute(sql_command)

        # 5. Get record count
        count = con.execute("SELECT COUNT(*) FROM trusted.trip_details").fetchone()[0]
        logger.info(f"Table created successfully with {count} records!")

        # 6. Export to MinIO
        trip_details_table_path = f"{bucket_name}/{app_folder}/trip_details"
        export_query = f"""
            COPY trusted.trip_details 
            TO 's3://{trip_details_table_path}/trip_details.parquet' (FORMAT PARQUET);
        """
        con.execute(export_query)
        logger.info(f"Table successfully exported to s3://{trip_details_table_path}/")

    except duckdb.CatalogException as e:
        logger.error(f"Catalog Error (Table/View issue): {e}")
        raise
    except duckdb.IOException as e:
        logger.error(f"IO Error (MinIO connection or File not found): {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise
    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed.")


# def create_trip_details_table(config):
#     def get_config(config):
#         bucket_name = config.get("TRUSTED_BUCKET")
#         app_folder = config.get("APP_FOLDER")
#         connection_data = {
#             "minio_endpoint": config["MINIO_ENDPOINT"],
#             "access_key": config["ACCESS_KEY"],
#             "secret_key": config["SECRET_KEY"],
#             "secure": False,
#         }
#         return bucket_name, app_folder, connection_data

#     bucket_name, app_folder, connection_data = get_config(config)
#     # 1. Connect to DuckDB (local file or :memory:)
#     con = duckdb.connect("gtfs.db")

#     # 2. Setup MinIO Credentials and HTTPFS
#     con.execute(f"""
#         INSTALL httpfs;
#         LOAD httpfs;
#         SET s3_endpoint='{connection_data["minio_endpoint"]}';
#         SET s3_access_key_id='{connection_data["access_key"]}';
#         SET s3_secret_access_key='{connection_data["secret_key"]}';
#         SET s3_use_ssl=false;                       -- Set to true if using HTTPS
#         SET s3_url_style='path';                    -- Required for MinIO
#     """)

#     # 3. Define views pointing to your Parquet files in MinIO
#     # This allows your SQL query to use 'trusted.stop_times' as if it were a local table
#     con.execute("CREATE SCHEMA IF NOT EXISTS trusted;")
#     stop_times_table_path = f"{bucket_name}/{app_folder}/stop_times"
#     con.execute(
#         f"CREATE OR REPLACE VIEW trusted.stop_times AS SELECT * FROM read_parquet('s3://{stop_times_table_path}/*.parquet');"
#     )
#     stops_table_path = f"{bucket_name}/{app_folder}/stops"
#     con.execute(
#         f"CREATE OR REPLACE VIEW trusted.stops AS SELECT * FROM read_parquet('s3://{stops_table_path}/*.parquet');"
#     )

#     # 4. Your CTAS Query
#     sql_command = """
#     CREATE TABLE IF NOT EXISTS trusted.trip_details AS
#     WITH trip_extremes AS (
#         SELECT
#             trip_id,
#             MAX(CASE WHEN start_rank = 1 THEN stop_id END) AS first_stop_id,
#             MAX(CASE WHEN end_rank = 1 THEN stop_id END) AS last_stop_id
#         FROM (
#             SELECT
#                 trip_id, stop_id,
#                 ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY stop_sequence ASC) as start_rank,
#                 ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY stop_sequence DESC) as end_rank
#             FROM trusted.stop_times
#         ) ranked_data
#         GROUP BY trip_id
#     ),
#     trip_metrics AS (
#         SELECT
#             te.trip_id,
#             te.first_stop_id,
#             s1.stop_name AS first_stop_name,
#             s1.stop_lat AS first_stop_lat,
#             s1.stop_lon AS first_stop_lon,
#             te.last_stop_id,
#             s2.stop_name AS last_stop_name,
#             s2.stop_lat AS last_stop_lat,
#             s2.stop_lon AS last_stop_lon,
#             -- Haversine would be more accurate, but keeping your Euclidean logic:
#             ROUND(SQRT(POW(s2.stop_lat - s1.stop_lat, 2) + POW(s2.stop_lon - s1.stop_lon, 2)) * 106428) AS trip_linear_distance
#         FROM trip_extremes te
#         JOIN trusted.stops s1 ON te.first_stop_id = s1.stop_id
#         JOIN trusted.stops s2 ON te.last_stop_id = s2.stop_id
#     )
#     SELECT
#         *,
#         (trip_linear_distance = 0) AS is_circular
#     FROM trip_metrics;
#     """

#     # 5. Execute
#     logger.info("Creating trusted.trip_details...")
#     con.execute(sql_command)
#     logger.info("Table created successfully!")

#     count = con.execute("SELECT COUNT(*) FROM trusted.trip_details").fetchone()[0]
#     logger.info(f"Total records: {count}")

#     # Export the table to a specific path in MinIO
#     trip_details_table_path = f"{bucket_name}/{app_folder}/trip_details"
#     export_query = f"""
#     COPY trusted.trip_details
#     TO 's3://{trip_details_table_path}/trip_details.parquet'
#     (FORMAT PARQUET);
#     """

#     con.execute(export_query)
#     logger.info("Table successfully exported to MinIO!")
