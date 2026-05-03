# test_kafka_feature_consumer.py

import json
import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')  # host port
KAFKA_TOPIC = os.getenv('KAFKA_FEATURE_TOPIC', 'kyc.completed')

# Test payload — swap aisubscription_id and tenant_id as needed
TEST_PAYLOAD = {
    "aisubscription_id": "SUB2026111100086",
    "tenant_id": "tnt20261111001"
}


def send_test_message():
    max_retries = 5
    retry_delay = 3

    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            break
        except NoBrokersAvailable as e:
            print(f"Attempt {attempt+1}: Kafka not ready: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(retry_delay)

    producer.send(KAFKA_TOPIC, value=TEST_PAYLOAD)
    producer.flush()
    producer.close()

    print(f"✅ [test_kafka_feature] Message sent to topic '{KAFKA_TOPIC}':")
    print(json.dumps(TEST_PAYLOAD, indent=2))


if __name__ == "__main__":
    send_test_message()