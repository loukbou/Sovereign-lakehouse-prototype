import random
from datetime import datetime, timezone
from uuid import uuid4


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_shipment_event() -> dict:
    payload = {
        "shipment_id": str(uuid4()),
        "vehicle_id": random.choice(["TRUCK_01", "TRUCK_02", "VAN_01", "VAN_02"]),
        "origin_city": random.choice(["Rabat", "Casablanca", "Benguerir", "Marrakech"]),
        "destination_city": random.choice(["Rabat", "Casablanca", "Benguerir", "Marrakech"]),
        "status": random.choice(["CREATED", "IN_TRANSIT", "DELIVERED", "CANCELLED"]),
        "weight_kg": round(random.uniform(1, 500), 2),
        "event_time": now_utc_iso(),
    }

    if random.random() < 0.2:
        error_type = random.choice(["missing_shipment_id", "bad_status", "negative_weight"])

        if error_type == "missing_shipment_id":
            payload["shipment_id"] = None
        elif error_type == "bad_status":
            payload["status"] = "UNKNOWN"
        elif error_type == "negative_weight":
            payload["weight_kg"] = -10.0

    return payload