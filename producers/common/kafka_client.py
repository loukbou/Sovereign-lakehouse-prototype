import io
import logging
import json
import requests
import fastavro

from confluent_kafka import Producer

logger = logging.getLogger(__name__)
APICURIO_URL = __import__("os").environ.get("APICURIO_URL", "http://apicurio:8080")


def fetch_avro_schema(group_id: str, artifact_id: str) -> dict:
    resp = requests.get(
        f"{APICURIO_URL}/apis/registry/v3/groups/{group_id}/artifacts/{artifact_id}/versions/branch=latest/content",
        headers={"Accept": "application/json"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


class KafkaProducerClient:
    def __init__(self, bootstrap_servers: str, group_id: str = None, artifact_id: str = None):
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 5,
        })
        self.parsed_schema = None
        if group_id and artifact_id:
            schema_dict = fetch_avro_schema(group_id, artifact_id)
            self.parsed_schema = fastavro.parse_schema(schema_dict)

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error("Delivery failed: %s", err)
        else:
            logger.info("Delivered to %s [%s] offset %s", msg.topic(), msg.partition(), msg.offset())

    def send_avro(self, topic: str, key: str, value: dict):
        if self.parsed_schema is None:
            raise RuntimeError("Avro schema not configured")
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, self.parsed_schema, value)
        self.producer.produce(topic=topic, key=key, value=buf.getvalue(), callback=self.delivery_report)
        self.producer.poll(0)

    def send_json(self, topic: str, key: str, value: dict):
        self.producer.produce(topic=topic, key=key, value=json.dumps(value).encode("utf-8"), callback=self.delivery_report)
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()