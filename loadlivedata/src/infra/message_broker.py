from kafka import KafkaConsumer
import json
from src.services.load_data_to_raw import load_data_to_raw


def start_consumer(broker, topic, bucket_name, app_folder):
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
            data_json = message.value
            print(f"--- New Message Received at {message.timestamp} ---")

            data = json.loads(data_json)

            if isinstance(data, dict):
                total_qv = 0
                for line in data.get("l", []):
                    # print(f"Line: {line.get('qv')}")
                    total_qv += int(line.get("qv", 0))
                print(
                    f"Received data for {total_qv} vehicles from {len(data.get('l', []))} bus lines."
                )
                load_data_to_raw(
                    data=data_json,
                    raw_bucket_name=bucket_name,
                    app_folder=app_folder,
                    hour_minute=data.get("payload")
                    .get("hr")
                    .replace(":", ""),  # e.g., "15:30" -> "1530"
                )

            else:
                print("Not a valid payload format.")
                # print(f"Payload: {payload}")
            num_read_messages += 1
            print(f"Total messages read: {num_read_messages}\n")
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()
