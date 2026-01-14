from kafka import KafkaProducer
import json
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)

# Stores our producer so we don't reconnect every time
_producer = None


def get_producer(broker):
    """
    Singleton-style function to manage the Kafka connection.
    """
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=broker,
            # We increase this limit just in case, but the broker must also allow it
            max_request_size=5242880,  # 5MB limit
            compression_type="gzip",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    return _producer


def sendKafka(topic, message, broker):
    """
    Sends a message to Kafka and catches errors (like message size limits).
    """
    try:
        producer = get_producer(broker)
    except Exception as e:
        logger.error(f"--- KAFKA CONNECTION ERROR ---")
        logger.error(f"Failed to connect to Kafka broker at {broker}.")
        logger.error(f"Error details: {e}")
        logger.error(f"------------------------------")
        return
    try:
        # .get(timeout=10) forces the code to wait for a success or failure
        # This prevents the 'silent failure' for large payloads
        future = producer.send(topic, message)
        record_metadata = future.get(timeout=10)

        logger.info(
            f"Successfully sent to {topic} [Partition: {record_metadata.partition}]"
        )

    except Exception as e:
        logger.error(f"--- KAFKA ERROR ---")
        logger.error(f"Failed to send message to topic {topic}.")
        logger.error(f"Error details: {e}")
        logger.error(f"-------------------")
