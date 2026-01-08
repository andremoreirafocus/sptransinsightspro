from kafka import KafkaConsumer
from dotenv import dotenv_values
import json


def start_consumer():
    # Load config for broker and topic
    config = dotenv_values(".env")
    broker = config.get("KAFKA_BROKER")
    topic = config.get("KAFKA_TOPIC")
    num_read_messages = 0

    print(f"[*] Connecting to {broker}...")

    # Initialize the Consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        # Automatically handle JSON decoding
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        # Start from the earliest messages if no offset is stored
        auto_offset_reset="earliest",
        # Group ID allows multiple consumers to work together
        group_id="bus_monitor_group",
    )

    print(f"[*] Waiting for messages on topic: {topic}. To exit press CTRL+C")
    print(f"Total messages read: {num_read_messages}\n")

    try:
        for message in consumer:
            # 'message.value' is now a Python dictionary thanks to value_deserializer
            payload = message.value
            print(f"--- New Message Received at {message.timestamp} ---")

            # If your payload is the 'teste' string or the bus dict:
            if isinstance(payload, dict):
                print(f"Received data for {len(payload.get('v', []))} vehicles.")
            else:
                print(f"Payload: {payload}")
            num_read_messages += 1
            print(f"Total messages read: {num_read_messages}\n")
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    start_consumer()
