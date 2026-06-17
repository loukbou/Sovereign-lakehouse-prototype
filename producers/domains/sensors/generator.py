import random
from datetime import datetime, timezone
from uuid import uuid4


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Sensors Domain ────────────────────────────────────────────────────────────

def generate_reading() -> dict:
    """Streaming: continuous sensor telemetry (temperature, vibration)."""
    payload = {
        "reading_id":      str(uuid4()),
        "sensor_id":       str(uuid4()),
        "machine_id":      str(uuid4()),
        "temperature":     round(random.uniform(15, 100), 2),
        "vibration_level": round(random.uniform(0, 10), 3),
        "event_time":      now_utc_iso(),
    }
    # Realistic anomalies (15% chance)
    if random.random() < 0.15:
        if random.random() < 0.5:
            payload["temperature"] = round(random.uniform(120, 200), 2)
        else:
            payload["vibration_level"] = round(random.uniform(15, 30), 3)

    # Intentional bad data for governance testing (20% chance)
    if random.random() < 0.2:
        error_type = random.choice(["missing_reading_id", "negative_temperature", "invalid_vibration"])
        if error_type == "missing_reading_id":
            payload["reading_id"] = None
        elif error_type == "negative_temperature":
            payload["temperature"] = -10.0
        elif error_type == "invalid_vibration":
            payload["vibration_level"] = -5.0
    return payload


def generate_machine() -> dict:
    """Batch: machine reference / status update (every 1h)."""
    payload = {
        "machine_id":   str(uuid4()),
        "machine_type": random.choice(["CNC", "ROBOT_ARM", "CONVEYOR_BELT", "PUMP", "COMPRESSOR"]),
        "site_id":      random.choice(["SITE_A", "SITE_B", "SITE_C", "SITE_D"]),
        "status":       random.choice(["RUNNING", "STOPPED", "MAINTENANCE", "OFFLINE"]),
        "installed_at": now_utc_iso(),
    }
    if random.random() < 0.2:
        error_type = random.choice(["missing_machine_id", "invalid_status", "missing_site"])
        if error_type == "missing_machine_id":
            payload["machine_id"] = None
        elif error_type == "invalid_status":
            payload["status"] = "UNKNOWN"
        elif error_type == "missing_site":
            payload["site_id"] = None
    return payload


def generate_alert() -> dict:
    """Streaming: sensor alert events."""
    payload = {
        "alert_id":   str(uuid4()),
        "sensor_id":  str(uuid4()),
        "machine_id": str(uuid4()),
        "severity":   random.choice(["INFO", "WARNING", "CRITICAL", "EMERGENCY"]),
        "alert_type": random.choice([
            "OVERHEATING", "EXCESSIVE_VIBRATION", "POWER_FAILURE",
            "COMMUNICATION_LOSS", "THRESHOLD_EXCEEDED",
        ]),
        "event_time": now_utc_iso(),
    }
    if random.random() < 0.2:
        error_type = random.choice(["missing_alert_id", "invalid_severity", "missing_machine_id"])
        if error_type == "missing_alert_id":
            payload["alert_id"] = None
        elif error_type == "invalid_severity":
            payload["severity"] = "INVALID"
        elif error_type == "missing_machine_id":
            payload["machine_id"] = None
    return payload


# ── Sales Domain ──────────────────────────────────────────────────────────────

def generate_transaction() -> dict:
    """Streaming: sales transaction events."""
    payload = {
        "transaction_id": str(uuid4()),
        "customer_id":    str(uuid4()),
        "product_id":     str(uuid4()),
        "amount":         round(random.uniform(1.0, 5000.0), 2),
        "currency":       random.choice(["EUR", "USD", "GBP"]),
        "status":         random.choice(["COMPLETED", "PENDING", "CANCELLED", "REFUNDED"]),
        "event_time":     now_utc_iso(),
    }
    if random.random() < 0.2:
        error_type = random.choice(["negative_amount", "invalid_currency", "missing_customer"])
        if error_type == "negative_amount":
            payload["amount"] = -50.0
        elif error_type == "invalid_currency":
            payload["currency"] = "INVALID"
        elif error_type == "missing_customer":
            payload["customer_id"] = None
    return payload


def generate_customer() -> dict:
    """Batch: customer reference data (every 1h)."""
    payload = {
        "customer_id": str(uuid4()),
        "segment":     random.choice(["RETAIL", "WHOLESALE", "ONLINE", "VIP"]),
        "country":     random.choice(["FR", "DE", "ES", "IT", "UK"]),
        "created_at":  now_utc_iso(),
    }
    if random.random() < 0.2:
        if random.random() < 0.5:
            payload["customer_id"] = None
        else:
            payload["segment"] = "UNKNOWN"
    return payload


# ── Logistics Domain ──────────────────────────────────────────────────────────

def generate_shipment_event() -> dict:
    """Streaming: shipment tracking events."""
    payload = {
        "shipment_id": str(uuid4()),
        "vehicle_id":  str(uuid4()),
        "status":      random.choice(["IN_TRANSIT", "DELIVERED", "DELAYED", "RETURNED"]),
        "latitude":    round(random.uniform(-90.0, 90.0), 6),
        "longitude":   round(random.uniform(-180.0, 180.0), 6),
        "event_time":  now_utc_iso(),
    }
    if random.random() < 0.2:
        error_type = random.choice(["invalid_status", "out_of_range_lat", "missing_shipment"])
        if error_type == "invalid_status":
            payload["status"] = "UNKNOWN"
        elif error_type == "out_of_range_lat":
            payload["latitude"] = 999.0
        elif error_type == "missing_shipment":
            payload["shipment_id"] = None
    return payload


def generate_vehicle() -> dict:
    """Batch: vehicle reference data (every 1h)."""
    payload = {
        "vehicle_id":   str(uuid4()),
        "vehicle_type": random.choice(["TRUCK", "VAN", "DRONE", "CARGO_SHIP"]),
        "capacity_kg":  round(random.uniform(500, 20000), 1),
        "registered_at": now_utc_iso(),
    }
    if random.random() < 0.2:
        if random.random() < 0.5:
            payload["vehicle_id"] = None
        else:
            payload["capacity_kg"] = -100.0
    return payload
