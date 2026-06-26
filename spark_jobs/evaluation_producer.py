import uuid
import random
from datetime import datetime, timedelta, timezone

VALID_SEVERITIES = [
    "INFO",
    "WARNING",
    "CRITICAL",
    "EMERGENCY"
]

INVALID_SEVERITIES = [
    "BAD",
    "UNKNOWN",
    "ERROR"
]

VALID_ALERT_TYPES = [
    "OVERHEATING",
    "EXCESSIVE_VIBRATION",
    "POWER_FAILURE",
    "COMMUNICATION_LOSS",
    "THRESHOLD_EXCEEDED"
]


def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()


def future_iso(hours=3):
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()


def generate_dataset(
        n_records=10000,
        missing_sensor_pct=10,
        future_time_pct=10,
        duplicate_pct=15,
        invalid_severity_pct=5):

    records = []

    n_missing_sensor = int(n_records * missing_sensor_pct / 100)
    n_future = int(n_records * future_time_pct / 100)
    n_duplicates = int(n_records * duplicate_pct / 100)
    n_invalid_severity = int(n_records * invalid_severity_pct / 100)

    duplicate_ids = [
        str(uuid.uuid4())
        for _ in range(n_duplicates)
    ]

    for i in range(n_records):

        record = {
            "alert_id": str(uuid.uuid4()),
            "sensor_id": str(uuid.uuid4()),
            "machine_id": str(uuid.uuid4()),
            "severity": random.choice(VALID_SEVERITIES),
            "alert_type": random.choice(VALID_ALERT_TYPES),
            "event_time": now_utc_iso()
        }

        records.append(record)

    # -------------------------
    # Inject missing sensor IDs
    # -------------------------
    for idx in random.sample(
            range(n_records),
            n_missing_sensor):

        records[idx]["sensor_id"] = ""

    # -------------------------
    # Inject future timestamps
    # -------------------------
    for idx in random.sample(
            range(n_records),
            n_future):

        records[idx]["event_time"] = future_iso()

    # -------------------------
    # Inject invalid severity
    # -------------------------
    for idx in random.sample(
            range(n_records),
            n_invalid_severity):

        records[idx]["severity"] = random.choice(
            INVALID_SEVERITIES
        )

    # -------------------------
    # Inject duplicates
    # -------------------------
    dup_targets = random.sample(
        range(n_records),
        n_duplicates
    )

    for idx, dup_id in zip(
            dup_targets,
            duplicate_ids):

        records[idx]["alert_id"] = dup_id

    return records
if __name__ == "__main__":

    records = generate_dataset(
        n_records=10000,
        missing_sensor_pct=10,
        future_time_pct=10,
        duplicate_pct=15,
        invalid_severity_pct=5
    )

    print(f"Generated {len(records)} records")

    for record in records[:5]:
        print(record)