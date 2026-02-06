def get_config():
    """
    Load configuration from .env file using dotenv_values.
    Returns a dictionary with configuration values.
    """
    config = {
        "POSITIONS_TABLE_NAME": "trusted.positions",
        "FINISHED_TRIPS_TABLE_NAME": "refined.finished_trips",
        "DB_HOST": "postgres",
        "DB_PORT": 5432,
        "DB_DATABASE": "sptrans_insights",
        "DB_USER": "postgres",
        "DB_PASSWORD": "postgres",
        "DB_SSLMODE": "prefer",
    }
    return config
