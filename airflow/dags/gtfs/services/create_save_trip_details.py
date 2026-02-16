from infra.duck_db import get_duckdb_connection
import logging

logger = logging.getLogger(__name__)


def create_trip_details_table_and_fill_missing_data(config):
    def get_config(config):
        try:
            bucket_name = config["TRUSTED_BUCKET"]
            app_folder = config["GTFS_FOLDER"]
            trip_details = config["TRIP_DETAILS_TABLE_NAME"]
            return bucket_name, app_folder, trip_details
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    try:
        bucket_name, app_folder, trip_details = get_config(config)
        con = get_duckdb_connection(config)
        stop_times_table_path = f"{bucket_name}/{app_folder}/stop_times"
        stops_table_path = f"{bucket_name}/{app_folder}/stops"
        con.execute(f"""
            CREATE OR REPLACE VIEW stop_times AS 
            SELECT * FROM read_parquet('s3://{stop_times_table_path}/*.parquet');
        """)
        con.execute(f"""
            CREATE OR REPLACE VIEW stops AS 
            SELECT * FROM read_parquet('s3://{stops_table_path}/*.parquet');
        """)
        sql_command = f"""
        CREATE TABLE {trip_details} AS
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
                FROM stop_times st
                JOIN stops s ON st.stop_id = s.stop_id
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
        logger.info("Generating trip_details...")
        con.execute(sql_command)
        count = con.execute(f"SELECT COUNT(*) FROM {trip_details}").fetchone()[0]
        logger.info(f"Table created successfully with {count} records!")
        trip_details_table_path = f"{bucket_name}/{app_folder}/{trip_details}"
        export_query = f"""
            COPY {trip_details} 
            TO 's3://{trip_details_table_path}/{trip_details}.parquet' (FORMAT PARQUET);
        """
        con.execute(export_query)
        logger.info(f"Table successfully exported to s3://{trip_details_table_path}/")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise
    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed.")
