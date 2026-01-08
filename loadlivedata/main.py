from dotenv import dotenv_values
from src.infra.message_broker import start_consumer

if __name__ == "__main__":
    config = dotenv_values(".env")
    broker = config.get("KAFKA_BROKER")
    topic = config.get("KAFKA_TOPIC")

    start_consumer(
        broker=config.get("KAFKA_BROKER"),
        topic=config.get("KAFKA_TOPIC"),
        bucket_name=config.get("RAW_BUCKET_NAME"),
        app_folder=config.get("APP_FOLDER"),
    )
