import random
from datetime import datetime, timezone
from uuid import uuid4


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_sensor_event() -> dict:
    payload = {
        "sensor_event_id": str(uuid4()),
        "sensor_id": random.choice(["SENSOR_TEMP_01", "SENSOR_TEMP_02", "SENSOR_PRESS_01"]),
        "machine_id": random.choice(["MACHINE_01", "MACHINE_02", "MACHINE_03"]),
        "temperature": round(random.uniform(10, 80), 2),
        "vibration_level": round(random.uniform(0, 10), 2),
        "event_time": now_utc_iso(),
    }

    if random.random() < 0.2:
        error_type = random.choice(["missing_sensor_id", "temperature_too_high", "temperature_too_low"])

        if error_type == "missing_sensor_id":
            payload["sensor_id"] = None
        elif error_type == "temperature_too_high":
            payload["temperature"] = 150.0
        elif error_type == "temperature_too_low":
            payload["temperature"] = -50.0

    return payload