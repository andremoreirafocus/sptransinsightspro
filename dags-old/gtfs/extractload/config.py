def get_config():
    """
    Load configuration from .env file using dotenv_values.
    Returns a dictionary with configuration values.
    """
    config = {
        "GTFS_URL": "http://www.sptrans.com.br/umbraco/Surface/PerfilDesenvolvedor/BaixarGTFS",
        "LOGIN": "andre.moreira.engineer",
        "PASSWORD": "T3st3@p1",
        "LOCAL_DOWNLOADS_FOLDER": "/tmp",
        "MINIO_ENDPOINT": "minio:9000",
        "ACCESS_KEY": "datalake",
        "SECRET_KEY": "datalake",
        "RAW_BUCKET_NAME": "raw",
        "APP_FOLDER": "gtfs",
    }
    return config
