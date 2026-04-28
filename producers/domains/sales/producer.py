import logging
import time

from common.event_envelope import wrap_event
from common.kafka_client import KafkaProducerClient
from domains.sales.generator import generate_sales_transaction


logger = logging.getLogger(__name__)

''''
calling sales generator
wrapping event
publishing to Kafka
defining sales event type
'''
def run(config: dict):
    kafka = KafkaProducerClient(config["bootstrap_servers"])

    topic = config["topic"]
    sleep_seconds = config.get("sleep_seconds", 2)

    logger.info("Starting sales producer")
    logger.info("Publishing to topic: %s", topic)

    while True:
        payload = generate_sales_transaction()

        event = wrap_event(
            domain="sales",
            event_type="sales_transaction_created",
            payload=payload,
        )

        key = payload.get("transaction_id") or event["event_id"]

        kafka.send_json(topic=topic, key=key, value=event)
        kafka.flush()

        logger.info("Sent event: %s", event)

        time.sleep(sleep_seconds)