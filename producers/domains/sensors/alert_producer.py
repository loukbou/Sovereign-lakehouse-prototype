"""
Alert Producer — Sensors Domain.

Single responsibility: call generate_alert() and publish the raw flat
payload to the Kafka topic via Avro serialization against Apicurio Registry.

Does NOT: read contracts, apply validation rules, transform or wrap payloads.
Any structural violation (Avro schema mismatch) is caught and published
to the dedicated error topic for observability.
"""

import logging
import time
import uuid

from common.kafka_client import KafkaProducerClient
from domains.sensors.generator import generate_alert

logger = logging.getLogger(__name__)


def run(config: dict) -> None:
    """
    Start the alert producer loop.

    Expected config keys:
        bootstrap_servers     : Kafka broker list
        schema_registry_url   : Apicurio Registry URL (Confluent-compat endpoint)
        schema_path           : Path to the Avro .avsc file for serialization
        topic                 : Target Kafka topic
        producer_error_topic  : Topic for serialization error records
        sleep_seconds         : Seconds to wait between events (default: 3)
    """
    with open(config["schema_path"], "r") as f:
        avro_schema_str = f.read()

    kafka = KafkaProducerClient(
        bootstrap_servers=config["bootstrap_servers"],
        schema_registry_url=config["schema_registry_url"],
        avro_schema_str=avro_schema_str,
    )

    topic        = config["topic"]
    error_topic  = config["producer_error_topic"]
    sleep_sec    = config.get("sleep_seconds", 3)

    logger.info("Alert producer started → topic: %s", topic)

    while True:
        payload = generate_alert()
        key = payload.get("alert_id") or str(uuid.uuid4())

        try:
            kafka.send_avro(topic=topic, key=key, value=payload)
            logger.debug("Published: %s", payload)

        except Exception as exc:
            error_record = {
                "error_stage":   "producer_avro_serialization",
                "error_type":    type(exc).__name__,
                "error_message": str(exc),
                "target_topic":  topic,
                "failed_event":  payload,
            }
            kafka.send_json(topic=error_topic, key=key, value=error_record)
            logger.error("Serialization failed — sent to error topic: %s", error_record)

        kafka.flush()
        time.sleep(sleep_sec)
