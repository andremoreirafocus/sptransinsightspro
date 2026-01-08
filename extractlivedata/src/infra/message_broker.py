from kafka import KafkaProducer
import json


def sendKafka2(topic, message, broker):
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    producer.send(topic, message)
    producer.flush()
    producer.close()


from kafka import KafkaProducer
import json

# This variable will store our producer so we don't reconnect every time
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
    producer = get_producer(broker)
    try:
        # .get(timeout=10) forces the code to wait for a success or failure
        # This prevents the 'silent failure' for large payloads
        future = producer.send(topic, message)
        record_metadata = future.get(timeout=10)

        print(f"Successfully sent to {topic} [Partition: {record_metadata.partition}]")

    except Exception as e:
        print(f"--- KAFKA ERROR ---")
        print(f"Failed to send message to topic {topic}.")
        print(f"Error details: {e}")
        print(f"-------------------")
