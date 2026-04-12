import logging
from sqlalchemy import create_engine, text

# Initialize logger
logger = logging.getLogger(__name__)


def save_finished_trips_to_db(config, trips_tuples):
    """
    Saves finished trips to Postgres using SQLAlchemy.
    Input: trips_tuples (List of Tuples)
    Tuple order: trip_id, vehicle_id, trip_start_time, trip_end_time, duration, is_circular, average_speed
    """

    def get_config(config):
        try:
            general = config["general"]
            tables = general["tables"]
            database = config["connections"]["database"]
            table_name = tables["finished_trips_table_name"]
            host = database["host"]
            port = database["port"]
            dbname = database["database"]
            dbuser = database["user"]
            password = database["password"]
            return (table_name, host, port, dbname, dbuser, password)
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise ValueError(f"Missing required configuration key: {e}")

    (table_name, host, port, dbname, dbuser, password) = get_config(config)
    db_uri = f"postgresql://{dbuser}:{password}@{host}:{port}/{dbname}"
    engine = create_engine(db_uri)
    staging_table = f"{table_name}_stg"
    logger.info(f"Using staging table: {staging_table} for batch operations.")
    try:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {staging_table};"))
            conn.execute(
                text(f"""
                CREATE UNLOGGED TABLE {staging_table} 
                AS SELECT * FROM {table_name} WITH NO DATA;
            """)
            )
            if trips_tuples:
                insert_stmt = text(f"""
                    INSERT INTO {staging_table} (
                        trip_id, vehicle_id, trip_start_time, trip_end_time, 
                        duration, is_circular, average_speed
                    ) VALUES (:t_id, :v_id, :t_start, :t_end, :dur, :circ, :spd)
                """)
                params = [
                    {
                        "t_id": t[0],
                        "v_id": t[1],
                        "t_start": t[2],
                        "t_end": t[3],
                        "dur": t[4],
                        "circ": t[5],
                        "spd": t[6],
                    }
                    for t in trips_tuples
                ]
                conn.execute(insert_stmt, params)
        with engine.begin() as conn:
            upsert_query = text(f"""
                INSERT INTO {table_name} (
                    trip_id, vehicle_id, trip_start_time, trip_end_time, 
                    duration, is_circular, average_speed
                )
                SELECT 
                    trip_id, vehicle_id, trip_start_time, trip_end_time, 
                    duration, is_circular, average_speed
                FROM {staging_table}
                ON CONFLICT (trip_start_time, vehicle_id, trip_id) 
                DO NOTHING;
            """)
            execution_result = conn.execute(upsert_query)
            new_rows = execution_result.rowcount
            skipped_rows = len(trips_tuples) - new_rows
            conn.execute(text(f"ANALYZE {table_name};"))
            logger.info(
                f"Sync complete: {new_rows} new trips added, {skipped_rows} duplicates skipped."
            )
    except Exception as e:
        logger.error(f"Persistence failed: {e}")
        raise ValueError(f"Persistence failed: {e}")
    finally:
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {staging_table};"))
        except Exception:
            pass
        engine.dispose()
