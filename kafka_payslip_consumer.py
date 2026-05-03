import os
import base64
import json
import logging
import tempfile
import time
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import requests

load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_PAYSLIP_TOPIC', 'payslip_extracted')
API_BASE_URL = os.getenv('API_BASE_URL', 'http://api:8118/api')
ENDPOINT = f"{API_BASE_URL}/aisubscriptiondocs/payslip_with_extracted"
API_KEY = os.getenv('API_KEY', 'your-api-key')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_message(msg_value: dict):
    try:
        tenant_id = msg_value['tenant_id']
        aisubscription_id = msg_value['aisubscription_id']
        remarks = msg_value.get('remarks')
        filename = msg_value['filename']
        file_base64 = msg_value['file_base64']
        extracted_data_json = msg_value['extracted_data_json']

        file_bytes = base64.b64decode(file_base64)

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name

        try:
            with open(tmp_path, 'rb') as f:
                files = {'file': (filename, f, 'application/octet-stream')}
                data = {
                    'tenant_id': tenant_id,
                    'aisubscription_id': aisubscription_id,
                    'extracted_data_json': extracted_data_json
                }
                if remarks:
                    data['remarks'] = remarks

                headers = {'X-API-Key': API_KEY}

                logger.info(f"Sending to {ENDPOINT} for {aisubscription_id}")
                resp = requests.post(ENDPOINT, data=data, files=files, headers=headers)

            if resp.status_code == 201:
                logger.info(f"Success: {resp.json()}")
            else:
                logger.error(f"Failed ({resp.status_code}): {resp.text}")
        finally:
            os.unlink(tmp_path)

    except Exception as e:
        logger.exception(f"Error processing message: {e}")

def process_messagev2(msg_value: dict):
    try:
        # --- existing extraction of fields ---
        tenant_id = msg_value['tenant_id']
        aisubscription_id = msg_value['aisubscription_id']
        remarks = msg_value.get('remarks')
        filename = msg_value['filename']
        file_base64 = msg_value['file_base64']
        extracted_data_json = msg_value['extracted_data_json']

        # NEW: retrieve correlation_id and reply_topic
        correlation_id = msg_value.get('correlation_id')
        reply_topic = msg_value.get('reply_topic')

        file_bytes = base64.b64decode(file_base64)
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name

        try:
            with open(tmp_path, 'rb') as f:
                files = {'file': (filename, f, 'application/octet-stream')}
                data = {
                    'tenant_id': tenant_id,
                    'aisubscription_id': aisubscription_id,
                    'extracted_data_json': extracted_data_json
                }
                if remarks:
                    data['remarks'] = remarks

                headers = {'X-API-Key': API_KEY}

                logger.info(f"Sending to {ENDPOINT} for {aisubscription_id}")
                resp = requests.post(ENDPOINT, data=data, files=files, headers=headers)

            if resp.status_code == 201:
                result = resp.json()
                doc_id = result.get("aisubscriptiondoc_id")
                logger.info(f"Success: {result}")
                reply_status = "success"
                reply_data = {
                    "correlation_id": correlation_id,
                    "status": "success",
                    "doc_id": doc_id
                }
            else:
                logger.error(f"Failed ({resp.status_code}): {resp.text}")
                reply_status = "error"
                reply_data = {
                    "correlation_id": correlation_id,
                    "status": "error",
                    "error": f"HTTP {resp.status_code}: {resp.text}"
                }
        finally:
            os.unlink(tmp_path)

        # --- Send reply if reply_topic and correlation_id provided ---
        if reply_topic and correlation_id:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            producer.send(reply_topic, value=reply_data)
            producer.flush()
            logger.info(f"Reply sent to {reply_topic} for correlation_id={correlation_id}")
            producer.close()

    except Exception as e:
        logger.exception(f"Error processing message: {e}")
        # Optionally send error reply as well
        if 'correlation_id' in msg_value and 'reply_topic' in msg_value:
            try:
                from kafka import KafkaProducer
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                producer.send(msg_value['reply_topic'], value={
                    "correlation_id": msg_value['correlation_id'],
                    "status": "error",
                    "error": str(e)
                })
                producer.flush()
                producer.close()
            except:
                pass


def main():
    # Retry connecting to Kafka
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
                group_id='payslip_consumer_group',
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