import logging
from infra.db import get_db_connection
from psycopg2 import DatabaseError, InterfaceError

logger = logging.getLogger(__name__)


def create_trip_details_table(config):
    """
    Executes the transformation to create the trusted.trip_details table.
    This creates the table based on stop_times and stops coordinates.
    """
    conn = None
    # We use the final optimized query we developed
    sql_command = """
    CREATE TABLE IF NOT EXISTS trusted.trip_details AS
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
            te.trip_id,
            te.first_stop_id,
            s1.stop_name AS first_stop_name,
            s1.stop_lat AS first_stop_lat,
            s1.stop_lon AS first_stop_lon,
            te.last_stop_id,
            s2.stop_name AS last_stop_name,
            s2.stop_lat AS last_stop_lat,
            s2.stop_lon AS last_stop_lon,
            ROUND(SQRT(POW(s2.stop_lat - s1.stop_lat, 2) + POW(s2.stop_lon - s1.stop_lon, 2)) * 106428) AS trip_linear_distance
        FROM trip_extremes te
        JOIN trusted.stops s1 ON te.first_stop_id = s1.stop_id
        JOIN trusted.stops s2 ON te.last_stop_id = s2.stop_id
    )
    SELECT 
        *,
        (trip_linear_distance = 0) AS is_circular
    FROM trip_metrics;
    """

    try:
        conn = get_db_connection(config)
        cur = conn.cursor()

        # We drop the table first if we want to refresh the summary
        cur.execute("DROP TABLE IF EXISTS trusted.trip_details;")

        # Execute the transformation
        cur.execute(sql_command)

        conn.commit()
        logger.info("Successfully created trusted.trip_details table.")

    except (DatabaseError, InterfaceError) as db_err:
        if conn:
            conn.rollback()
        logger.error(f"Database error during trip_details creation: {db_err}")
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Unexpected error: {e}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()


def create_trip_details_table_and_fill_missing_data(config):
    conn = None
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
    try:
        conn = get_db_connection(config)
        cur = conn.cursor()
        cur.execute(sql_command)
        conn.commit()
        logger.info("Successfully created and healed trusted.trip_details table.")
    except (DatabaseError, InterfaceError) as db_err:
        if conn:
            conn.rollback()
        logger.error(f"Database error during table creation: {db_err}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()


def create_trip_details_table_and_fill_missing_data_only_non_circular(config):
    """
    Creates and heals the trusted.trip_details table.
    Ensures return trips (-1) are generated by mirroring forward trips (-0)
    if the return data is missing from the source.
    """
    conn = None

    # We use a CTE to define the base math, then a UNION to patch the missing trips
    sql_command = """
    DROP TABLE IF EXISTS trusted.trip_details;
    
    CREATE TABLE trusted.trip_details AS
    WITH base_metrics AS (
        -- Step 1: Standard calculation from stop_times
        SELECT 
            trip_id,
            MAX(CASE WHEN start_rank = 1 THEN stop_id END) AS first_stop_id,
            MAX(CASE WHEN start_rank = 1 THEN stop_lat END) AS  first_stop_lat,
            MAX(CASE WHEN start_rank = 1 THEN stop_lon END) AS  first_stop_lon,
            MAX(CASE WHEN end_rank = 1 THEN stop_id END) AS last_stop_id,
            MAX(CASE WHEN end_rank = 1 THEN stop_lat END) AS  last_stop_lat,
            MAX(CASE WHEN end_rank = 1 THEN stop_lon END) AS  last_stop_lon
        FROM (
            SELECT 
                st.trip_id, st.stop_id, s.stop_lat, s.stop_lon,
                ROW_NUMBER() OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence ASC) as start_rank,
                ROW_NUMBER() OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence DESC) as end_rank
            FROM trusted.stop_times st
            JOIN trusted.stops s ON st.stop_id = s.stop_id
        ) ranked_data
        GROUP BY trip_id
    ),
    calculated_base AS (
        -- Step 2: Add distance and circular flag to found data
        SELECT 
            *,
            ROUND(SQRT(POW( last_stop_lat -  first_stop_lat, 2) + POW( last_stop_lon -  first_stop_lon, 2)) * 106428) AS trip_linear_distance,
            (ROUND(SQRT(POW( last_stop_lat -  first_stop_lat, 2) + POW( last_stop_lon -  first_stop_lon, 2)) * 106428) = 0) AS is_circular
        FROM base_metrics
    ),
    missing_patches AS (
        -- Step 3: Identify missing -1 trips and swap coordinates
        SELECT 
            REPLACE(t0.trip_id, '-0', '-1') AS trip_id,
            t0.last_stop_id AS first_stop_id,
            t0. last_stop_lat AS  first_stop_lat,
            t0. last_stop_lon AS  first_stop_lon,
            t0.first_stop_id AS last_stop_id,
            t0. first_stop_lat AS  last_stop_lat,
            t0. first_stop_lon AS  last_stop_lon,
            t0.trip_linear_distance,
            FALSE AS is_circular
        FROM calculated_base t0
        LEFT JOIN calculated_base t1 ON t1.trip_id = REPLACE(t0.trip_id, '-0', '-1')
        WHERE t0.trip_id LIKE '%-0' 
          AND t0.is_circular IS FALSE 
          AND t1.trip_id IS NULL
    )
    SELECT * FROM calculated_base
    UNION ALL
    SELECT * FROM missing_patches;
    """

    try:
        conn = get_db_connection(config)
        cur = conn.cursor()
        cur.execute(sql_command)
        conn.commit()
        logger.info("Successfully created and healed trusted.trip_details table.")
    except (DatabaseError, InterfaceError) as db_err:
        if conn:
            conn.rollback()
        logger.error(f"Database error during table creation: {db_err}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()
