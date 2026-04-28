import logging
import time

from common.event_envelope import wrap_event
from common.kafka_client import KafkaProducerClient
from domains.logistics.generator import generate_shipment_event


logger = logging.getLogger(__name__)


def run(config: dict):
    kafka = KafkaProducerClient(config["bootstrap_servers"])

    topic = config["topic"]
    sleep_seconds = config.get("sleep_seconds", 3)

    logger.info("Starting logistics producer")
    logger.info("Publishing to topic: %s", topic)

    while True:
        payload = generate_shipment_event()

        event = wrap_event(
            domain="logistics",
            event_type="shipment_status_updated",
            payload=payload,
        )

        key = payload.get("shipment_id") or event["event_id"]

        kafka.send_json(topic=topic, key=key, value=event)
        kafka.flush()

        logger.info("Sent event: %s", event)

        time.sleep(sleep_seconds)