import uuid
import json
import requests
from datetime import datetime, timedelta, timezone
import random

KAFKA_BOOTSTRAP = "kafka1:9092"
TOPIC           = "bronze.sensors.alerts"
APICURIO_URL    = "http://apicurio:8080"
ARTIFACT_GROUP  = "sensors"
ARTIFACT_ID     = "alerts-schema"
NUM_RECORDS     = 10000
n_completeness = int(NUM_RECORDS * 0.10)
n_timeliness   = int(NUM_RECORDS * 0.10)
n_duplicates   = int(NUM_RECORDS * 0.15)

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

    records = []

    now = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%f+00:00"
    )

    future = (
        datetime.now(timezone.utc) + timedelta(hours=3)
    ).strftime(
        "%Y-%m-%dT%H:%M:%S.%f+00:00"
    )

    for _ in range(NUM_RECORDS):
        records.append({
            "alert_id": str(uuid.uuid4()),
            "sensor_id": str(uuid.uuid4()),
            "machine_id": str(uuid.uuid4()),
            "severity": "WARNING",
            "alert_type": "OVERHEATING",
            "event_time": now,
        })

    # 10% missing sensor_id
    for idx in random.sample(range(NUM_RECORDS),  n_completeness):
        records[idx]["sensor_id"] = ""

    # 10% future timestamps
    remaining = [i for i in range(NUM_RECORDS) if records[i]["sensor_id"] != ""]
    for idx in random.sample(remaining, n_timeliness):
        records[idx]["event_time"] = future


    # Select records that will become duplicates
    duplicate_targets = random.sample(
        range(NUM_RECORDS),
        n_duplicates
    )

    # Select unique source records
    duplicate_sources = random.sample(
        [i for i in range(NUM_RECORDS) if i not in duplicate_targets],
        n_duplicates
    )

    for src, dst in zip(duplicate_sources, duplicate_targets):
        records[dst]["alert_id"] = records[src]["alert_id"]
    from collections import Counter

    counts = Counter(r["alert_id"] for r in records)

    distinct_ids = len(counts)
    uniqueness_pct = round(distinct_ids * 100 / len(records), 2)

    print("\n=== PRODUCER DATASET STATS ===")
    print(f"Total records      : {len(records)}")
    print(f"Distinct alert IDs : {distinct_ids}")
    print(f"Uniqueness (%)     : {uniqueness_pct}")

    print("\nTop duplicates:")
    for aid, cnt in counts.most_common(10):
        if cnt > 1:
            print(aid, cnt)

    return records
def produce(schema_str, records):
    print("\n=== EXPECTED QUALITY PROFILE ===")
    print(f"Total Records            : {NUM_RECORDS}")
    print(f"Completeness Violations  : {n_completeness}")
    print(f"Timeliness Violations    : {n_timeliness}")
    print(f"Duplicate Records        : {n_duplicates}")
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
    print(f"  {n_completeness}x empty sensor_id")
    print(f"  {n_timeliness}x future event_time")
    print(f"  {n_duplicates}x duplicate alert_id")

if __name__ == "__main__":
    print(f"Fetching schema from Apicurio ({ARTIFACT_GROUP}/{ARTIFACT_ID})...")
    schema_str = fetch_schema()
    print("Schema fetched.")
    records = build_bad_records()
    print(f"Built {len(records)} bad records. Injecting into {TOPIC}...")
    produce(schema_str, records)