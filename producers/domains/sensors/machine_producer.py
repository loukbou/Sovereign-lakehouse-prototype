import logging
import time

from common.event_envelope import wrap_event
from common.kafka_client import KafkaProducerClient
from domains.sensors.generator import generate_machine

logger = logging.getLogger(__name__)

def run(config: dict):
    with open(config["schema_path"], "r") as f:
        avro_schema_str = f.read()
    
    kafka = KafkaProducerClient(
        bootstrap_servers=config["bootstrap_servers"],
        schema_registry_url=config["schema_registry_url"],
        avro_schema_str=avro_schema_str,
    )
    
    topic = config["topic"]
    sleep_seconds = config.get("sleep_seconds", 5)  # Batch/reference rate
    
    logger.info("Starting sensor machine batch/reference producer with Avro + Schema Registry")
    logger.info("Publishing to topic: %s", topic)
    
    error_topic = config["producer_error_topic"]
    
    while True:
        payload = generate_machine()
        
        event = wrap_event(
            domain="sensors",
            event_type="machine_updated",
            payload=payload,
        )
        
        key = payload.get("machine_id") or event["event_id"]
        
        try:
            kafka.send_avro(topic=topic, key=key, value=event)
            logger.info("Sent Avro event: %s", event)
        
        except Exception as e:
            error_record = {
                "error_stage": "producer_schema_serialization",
                "error_type": type(e).__name__,
                "error_message": str(e),
                "target_topic": topic,
                "failed_event": event,
            }
            
            kafka.send_json(
                topic=error_topic,
                key=event["event_id"],
                value=error_record,
            )
            
            logger.error("Schema serialization failed. Sent to producer error topic: %s", error_record)
        
        kafka.flush()
        time.sleep(sleep_seconds)