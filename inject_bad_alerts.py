import uuid
import json
import requests
from datetime import datetime, timedelta, timezone

KAFKA_BOOTSTRAP = "kafka1:9092"
TOPIC           = "bronze.sensors.alerts"
APICURIO_URL    = "http://apicurio:8080"
ARTIFACT_GROUP  = "sensors"
ARTIFACT_ID     = "alerts-schema"
NUM_RECORDS     = 100

def fetch_schema():
    url = (
        f"{APICURIO_URL}/apis/registry/v3/groups/{ARTIFACT_GROUP}"
        f"/artifacts/{ARTIFACT_ID}/versions/branch=latest/content"
    )
    r = requests.get(url, headers={"Accept": "application/json"}, timeout=10)
    r.raise_for_status()
    return r.text

def avro_encode(schema_str, record):
    try:
        import fastavro, io
        schema = fastavro.parse_schema(json.loads(schema_str))
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, schema, record)
        return buf.getvalue()
    except ImportError:
        pass
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import SerializationContext, MessageField
    sr = SchemaRegistryClient({"url": f"{APICURIO_URL}/apis/ccompat/v7"})
    serializer = AvroSerializer(sr, schema_str)
    return serializer(record, SerializationContext(TOPIC, MessageField.VALUE))

def build_bad_records():
    future = (datetime.now(timezone.utc) + timedelta(hours=3)).strftime(
        "%Y-%m-%dT%H:%M:%S.%f+00:00"
    )
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    records = []
    for i in range(NUM_RECORDS):
        if i % 2 == 0:
            # Empty sensor_id — valid Avro string, breaks has(sensor_id)
            records.append({
                "alert_id":   str(uuid.uuid4()),
                "sensor_id":  "",                   # NOT_EMPTY_VIOLATION
                "machine_id": str(uuid.uuid4()),
                "severity":   "WARNING",
                "alert_type": "OVERHEATING",
                "event_time": now,
            })
        else:
            # Future timestamp — valid Avro string, breaks event_time < now()
            records.append({
                "alert_id":   str(uuid.uuid4()),
                "sensor_id":  str(uuid.uuid4()),
                "machine_id": str(uuid.uuid4()),
                "severity":   "CRITICAL",
                "alert_type": "POWER_FAILURE",
                "event_time": future,               # NOT_FUTURE_VIOLATION
            })
    return records

def produce(schema_str, records):
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    ok = 0
    for rec in records:
        try:
            payload = avro_encode(schema_str, rec)
            p.produce(topic=TOPIC, key=rec["alert_id"].encode(), value=payload)
            p.poll(0)
            ok += 1
        except Exception as e:
            print(f"[SKIP] {e}")
    p.flush()
    print(f"\nInjected {ok}/{len(records)} bad records into {TOPIC}")
    print("  50x empty sensor_id   -> NOT_EMPTY_VIOLATION")
    print("  50x future event_time -> NOT_FUTURE_VIOLATION")
    print("  Wait ~30s for Spark batch to route them to quarantine.\n")

if __name__ == "__main__":
    print(f"Fetching schema from Apicurio ({ARTIFACT_GROUP}/{ARTIFACT_ID})...")
    schema_str = fetch_schema()
    print("Schema fetched.")
    records = build_bad_records()
    print(f"Built {len(records)} bad records. Injecting into {TOPIC}...")
    produce(schema_str, records)