import random
from datetime import datetime, timezone
from uuid import uuid4

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def generate_reading() -> dict:
    """Generate sensor reading event (streaming)"""
    payload = {
        "reading_id": str(uuid4()),
        "sensor_id": str(uuid4()),
        "machine_id": str(uuid4()),
        "temperature": round(random.uniform(15, 100), 2),  # Celsius
        "vibration_level": round(random.uniform(0, 10), 3),
        "event_time": now_utc_iso(),
    }
    
    # Inject anomalies
    if random.random() < 0.15:
        if random.random() < 0.5:
            payload["temperature"] = round(random.uniform(120, 200), 2)  # Overheating
        else:
            payload["vibration_level"] = round(random.uniform(15, 30), 3)  # Excessive vibration
    
    # Intentionally inject bad data (20% chance)
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
    """Generate sensor machine reference/update (batch)"""
    payload = {
        "machine_id": str(uuid4()),
        "machine_type": random.choice(["CNC", "ROBOT_ARM", "CONVEYOR_BELT", "PUMP", "COMPRESSOR"]),
        "site_id": random.choice(["SITE_A", "SITE_B", "SITE_C", "SITE_D"]),
        "status": random.choice(["RUNNING", "STOPPED", "MAINTENANCE", "OFFLINE"]),
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
    """Generate sensor alert event (streaming)"""
    payload = {
        "alert_id": str(uuid4()),
        "sensor_id": str(uuid4()),
        "machine_id": str(uuid4()),
        "severity": random.choice(["INFO", "WARNING", "CRITICAL", "EMERGENCY"]),
        "alert_type": random.choice([
            "OVERHEATING", 
            "EXCESSIVE_VIBRATION", 
            "POWER_FAILURE", 
            "COMMUNICATION_LOSS",
            "THRESHOLD_EXCEEDED"
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