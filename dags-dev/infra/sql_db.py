from sqlalchemy import create_engine
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def save_dataframe_to_db(config, latest_positions_df):
    def get_config(config):
        try:
            latest_positions_table_name = config["LATEST_POSITIONS_TABLE_NAME"]
            host = config["DB_HOST"]
            port = config["DB_PORT"]
            dbname = config["DB_DATABASE"]
            dbuser = config["DB_USER"]
            password = config["DB_PASSWORD"]
            return (latest_positions_table_name, host, port, dbname, dbuser, password)
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    (latest_positions_table_name, host, port, dbname, dbuser, password) = get_config(
        config
    )
    db_uri = f"postgresql://{dbuser}:{password}@{host}:{port}/{dbname}"
    engine = create_engine(db_uri)
    latest_positions_df.to_sql(
        name=latest_positions_table_name,
        con=engine,
        schema="refined",
        if_exists="replace",
        index=False,
    )
