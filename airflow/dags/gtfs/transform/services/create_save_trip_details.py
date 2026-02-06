import duckdb
import logging

logger = logging.getLogger(__name__)


def create_trip_details_table_and_fill_missing_data(config):
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
        DROP TABLE IF EXISTS trusted.trip_details;
        
        CREATE TABLE trusted.trip_details AS
        WITH base_metrics AS (
            SELECT 
                trip_id,
                MAX(CASE WHEN start_rank = 1 THEN stop_id END) AS first_stop_id,
                MAX(CASE WHEN start_rank = 1 THEN stop_name END) AS first_stop_name,
                MAX(CASE WHEN start_rank = 1 THEN stop_lat END) AS  first_stop_lat,
                MAX(CASE WHEN start_rank = 1 THEN stop_lon END) AS  first_stop_lon,
                MAX(CASE WHEN end_rank = 1 THEN stop_id END) AS last_stop_id,
                MAX(CASE WHEN end_rank = 1 THEN stop_name END) AS last_stop_name,
                MAX(CASE WHEN end_rank = 1 THEN stop_lat END) AS  last_stop_lat,
                MAX(CASE WHEN end_rank = 1 THEN stop_lon END) AS  last_stop_lon
            FROM (
                SELECT 
                    st.trip_id, st.stop_id,  s.stop_name, s.stop_lat, s.stop_lon,
                    ROW_NUMBER() OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence ASC) as start_rank,
                    ROW_NUMBER() OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence DESC) as end_rank
                FROM trusted.stop_times st
                JOIN trusted.stops s ON st.stop_id = s.stop_id
            ) ranked_data
            GROUP BY trip_id
        ),
        calculated_base AS (
            SELECT 
                *,
                ROUND(SQRT(POW( last_stop_lat -  first_stop_lat, 2) + POW( last_stop_lon -  first_stop_lon, 2)) * 106428) AS trip_linear_distance,
                (first_stop_id = last_stop_id) AS is_circular
            FROM base_metrics
        ),
        missing_patches AS (
            SELECT 
                REPLACE(t0.trip_id, '-0', '-1') AS trip_id,
                -- Logic: If circular, keep same. If not circular, swap.
                CASE WHEN t0.is_circular THEN t0.first_stop_id ELSE t0.last_stop_id END AS first_stop_id,
                CASE WHEN t0.is_circular THEN t0.first_stop_name ELSE t0.last_stop_name END AS first_stop_name,
                CASE WHEN t0.is_circular THEN t0. first_stop_lat ELSE t0. last_stop_lat END AS  first_stop_lat,
                CASE WHEN t0.is_circular THEN t0. first_stop_lon ELSE t0. last_stop_lon END AS  first_stop_lon,
                CASE WHEN t0.is_circular THEN t0.last_stop_id ELSE t0.first_stop_id END AS last_stop_id,
                CASE WHEN t0.is_circular THEN t0.last_stop_name ELSE t0.first_stop_name END AS last_stop_name,
                CASE WHEN t0.is_circular THEN t0. last_stop_lat ELSE t0. first_stop_lat END AS  last_stop_lat,
                CASE WHEN t0.is_circular THEN t0. last_stop_lon ELSE t0. first_stop_lon END AS  last_stop_lon,
                t0.trip_linear_distance,
                t0.is_circular
            FROM calculated_base t0
            LEFT JOIN calculated_base t1 ON t1.trip_id = REPLACE(t0.trip_id, '-0', '-1')
            WHERE t0.trip_id LIKE '%-0' AND t1.trip_id IS NULL
        )
        SELECT * FROM calculated_base
        UNION ALL
        SELECT * FROM missing_patches;
        """
        # ... (Rest of the psycopg2 execution code remains the same)

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


# deprecated
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
