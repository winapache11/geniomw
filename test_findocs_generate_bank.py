import json
import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
KAFKA_TOPIC = os.getenv('KAFKA_FINDOC_GENERATE_TOPIC', 'findoc_generate')

TEST_PAYLOAD = {
    "tenant_id": "tnt20261111001",
    "aisubscription_id": "SUB2026111100085",
    "document_type": "bank_statement",
    "num_records": 25,
    "num_months": 4,
    "risk": "medium_high",
}


def send_test_message(payload: dict = TEST_PAYLOAD):
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

    producer.send(KAFKA_TOPIC, value=payload)
    producer.flush()
    producer.close()

    print(f"✅ [test_findocs_generate] Message sent to topic '{KAFKA_TOPIC}':")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    send_test_message()
