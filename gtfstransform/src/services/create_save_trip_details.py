import logging
from src.infra.db import get_db_connection
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
            s1.stop_lat AS first_stop_lat,
            s1.stop_lon AS first_stop_lon,
            te.last_stop_id,
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
