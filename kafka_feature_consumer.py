# kafka_feature_consumer.py

import os
import json
import logging
import time
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import requests

load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_FEATURE_TOPIC', 'kyc.completed')
API_BASE_URL = os.getenv('API_BASE_URL', 'http://api:8118/api')
API_KEY = os.getenv('API_KEY', 'API20261111001')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_message(msg_value: dict):
    try:
        tenant_id = msg_value.get('tenant_id')
        aisubscription_id = msg_value.get('aisubscription_id')
        
        print(f"\n[feature_consumer] Received message for tenant_id={tenant_id}, aisubscription_id={aisubscription_id}\n")

        if not aisubscription_id or not tenant_id:
            logger.warning(f"\n[feature_consumer] invalid payload: {msg_value}\n")
            return

        ENDPOINT = f"{API_BASE_URL}/aifeatures/{aisubscription_id}"

        payload = {
            "tenant_id": tenant_id
        }

        headers = {
            "Content-Type": "application/json",
            "X-API-Key": API_KEY
        }

        logger.info(f"\n[feature_consumer] POST {ENDPOINT} payload={payload}\n")

        resp = requests.post(ENDPOINT, json=payload, headers=headers, timeout=10)

        if resp.status_code == 200:
            logger.info(f"[feature_consumer] success: {resp.json()}")
        else:
            logger.error(f"[feature_consumer] failed ({resp.status_code}): {resp.text}")

    except Exception as e:
        logger.exception(f"[feature_consumer] error processing message: {e}")


def main():
    max_retries = 10
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt+1}: Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='feature_consumer_group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            logger.info("Successfully connected to Kafka")
            break
        except NoBrokersAvailable as e:
            logger.warning(f"Kafka not ready yet: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(retry_delay)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(retry_delay)

    logger.info(f"Listening on topic: {KAFKA_TOPIC}")
    for msg in consumer:
        logger.info(f"Received message: {msg.value}")
        process_message(msg.value)


if __name__ == "__main__":
    main()