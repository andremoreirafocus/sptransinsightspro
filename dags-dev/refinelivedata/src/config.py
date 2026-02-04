from dotenv import dotenv_values


def get_config():
    """
    Load configuration from .env file using dotenv_values.
    Returns a dictionary with configuration values.
    """

    return dotenv_values(".env")
