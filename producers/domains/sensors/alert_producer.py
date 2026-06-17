import logging
import time
import uuid

from common.kafka_client import KafkaProducerClient
from domains.sensors.generator import generate_alert

logger = logging.getLogger(__name__)


def run(config: dict) -> None:

    kafka = KafkaProducerClient(
        bootstrap_servers=config["bootstrap_servers"],
        group_id=config.get("group_id", "sensors"),
        artifact_id=config.get("artifact_id", "alerts-schema"),
    )

    topic       = config["topic"]
    error_topic = config["producer_error_topic"]
    sleep_sec   = config.get("sleep_seconds", 3)

    logger.info("Alert producer started → topic: %s", topic)

    while True:
        payload = generate_alert()
        key = payload.get("alert_id") or str(uuid.uuid4())

        try:
            kafka.send_avro(
                topic=topic,
                key=key,
                value=payload
            )

        except Exception as exc:
            error_record = {
                "error_stage": "producer_avro_serialization",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "target_topic": topic,
                "failed_event": payload,
            }

            kafka.send_json(
                topic=error_topic,
                key=key,
                value=error_record
            )

        kafka.flush()
        time.sleep(sleep_sec)