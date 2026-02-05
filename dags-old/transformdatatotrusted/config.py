def get_config():
    """
    Load configuration from .env file using dotenv_values.
    Returns a dictionary with configuration values.
    """
    config = {
        "SOURCE_BUCKET": "raw",
        "APP_FOLDER": "sptrans",
        "POSITIONS_TABLE_NAME": "trusted.positions",
        "TRIP_DETAILS_TABLE_NAME": "trusted.trip_details",
        "MINIO_ENDPOINT": "minio:9000",
        "ACCESS_KEY": "datalake",
        "SECRET_KEY": "datalake",
        "DB_HOST": "postgres",
        "DB_PORT": 5432,
        "DB_DATABASE": "sptrans_insights",
        "DB_USER": "postgres",
        "DB_PASSWORD": "postgres",
        "DB_SSLMODE": "prefer",
    }
    return config
