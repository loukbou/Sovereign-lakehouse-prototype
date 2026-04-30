import logging
import json

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


logger = logging.getLogger(__name__)


class KafkaProducerClient:
    # initializes Avro serialization with Schema Registry
    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str | None = None,
        avro_schema_str: str | None = None,
    ):
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 5,
        })

        self.avro_serializer = None

        if schema_registry_url and avro_schema_str:
            schema_registry_client = SchemaRegistryClient({
                "url": schema_registry_url
            })

            self.avro_serializer = AvroSerializer(
                schema_registry_client,
                avro_schema_str
            )

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

    def send_avro(self, topic: str, key: str, value: dict):
        if self.avro_serializer is None:
            raise RuntimeError("Avro serializer is not configured")

        serialized_value = self.avro_serializer(
            value,
            SerializationContext(topic, MessageField.VALUE)
        )

        self.producer.produce(
            topic=topic,
            key=key,
            value=serialized_value,
            callback=self.delivery_report,
        )
        self.producer.poll(0)
    
    def send_json_bytes(self, topic: str, key: str, value: bytes):
        self.producer.produce(
            topic=topic,
            key=key,
            value=value,
            callback=self.delivery_report,
        )
        self.producer.poll(0)
    def send_json(self, topic: str, key: str, value: dict):
        self.send_json_bytes(
            topic=topic,
            key=key,
            value=json.dumps(value).encode("utf-8")
        )

    def flush(self):
        self.producer.flush()