import logging
import time

from common.event_envelope import wrap_event
from common.kafka_client import KafkaProducerClient
from domains.sensors.generator import generate_sensor_event


logger = logging.getLogger(__name__)


def run(config: dict):
    kafka = KafkaProducerClient(config["bootstrap_servers"])

    topic = config["topic"]
    sleep_seconds = config.get("sleep_seconds", 1)

    logger.info("Starting sensors producer")
    logger.info("Publishing to topic: %s", topic)

    while True:
        payload = generate_sensor_event()

        event = wrap_event(
            domain="sensors",
            event_type="machine_sensor_reading",
            payload=payload,
        )

        key = payload.get("sensor_event_id") or event["event_id"]

        kafka.send_json(topic=topic, key=key, value=event)
        kafka.flush()

        logger.info("Sent event: %s", event)

        time.sleep(sleep_seconds)