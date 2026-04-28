import json
import logging
from confluent_kafka import Producer


logger = logging.getLogger(__name__)
"""
Shared Kafka wrapper.
"""

class KafkaProducerClient:
    def __init__(self, bootstrap_servers: str):
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 5,
        })

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error("Delivery failed: %s", err)
        else:
            logger.info(
                "Delivered to %s [%s] offset %s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def send_json(self, topic: str, key: str, value: dict):
        self.producer.produce(
            topic=topic,
            key=key,
            value=json.dumps(value).encode("utf-8"),
            callback=self.delivery_report,
        )
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()
