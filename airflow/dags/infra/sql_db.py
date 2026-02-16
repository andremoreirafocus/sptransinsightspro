from sqlalchemy import create_engine, text
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def save_dataframe_to_db(config, df, full_table_name):
    def get_config(config):
        try:
            host = config["DB_HOST"]
            port = config["DB_PORT"]
            dbname = config["DB_DATABASE"]
            dbuser = config["DB_USER"]
            password = config["DB_PASSWORD"]
            return (host, port, dbname, dbuser, password)
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    (host, port, dbname, dbuser, password) = get_config(config)
    db_uri = f"postgresql://{dbuser}:{password}@{host}:{port}/{dbname}"
    engine = create_engine(db_uri)
    schema = full_table_name.split(".")[0]
    table_name = full_table_name.split(".")[1]
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
    )


def update_db_table_with_dataframe(config, df, full_table_name):
    def get_config(config):
        try:
            host = config["DB_HOST"]
            port = config["DB_PORT"]
            dbname = config["DB_DATABASE"]
            dbuser = config["DB_USER"]
            password = config["DB_PASSWORD"]
            return (host, port, dbname, dbuser, password)
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    (host, port, dbname, dbuser, password) = get_config(config)
    db_uri = f"postgresql://{dbuser}:{password}@{host}:{port}/{dbname}"
    schema = full_table_name.split(".")[0]
    table_name = full_table_name.split(".")[1]
    engine = create_engine(db_uri)
    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE refined."{table_name}"'))
        df.to_sql(
            name=table_name,
            con=conn,
            schema=schema,
            if_exists="append",
            index=False,
        )
        # Update statistics so query stays fast
        conn.execute(text(f'ANALYZE {schema}."{table_name}"'))
