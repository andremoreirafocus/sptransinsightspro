from kafka import KafkaProducer
import json

def sendKafka(topic, message, broker="kafka-broker:29092"):
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    producer.send(topic, message)
    producer.flush()
    producer.close()
