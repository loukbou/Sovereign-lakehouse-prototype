import random
from datetime import datetime, timezone
from uuid import uuid4

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def generate_shipment() -> dict:
    """Generate logistics shipment event (streaming)"""
    payload = {
        "shipment_id": str(uuid4()),
        "vehicle_id": str(uuid4()),
        "origin_city": random.choice(["Rabat", "Casablanca", "Tangier", "Marrakech", "Fez"]),
        "destination_city": random.choice(["Rabat", "Casablanca", "Tangier", "Marrakech", "Fez"]),
        "status": random.choice(["PENDING", "IN_TRANSIT", "DELIVERED", "DELAYED", "CANCELLED"]),
        "weight_kg": round(random.uniform(100, 5000), 2),
        "event_time": now_utc_iso(),
    }
    
    # Ensure origin != destination
    while payload["origin_city"] == payload["destination_city"]:
        payload["destination_city"] = random.choice(["Rabat", "Casablanca", "Tangier", "Marrakech", "Fez"])
    
    # Intentionally inject bad data (20% chance)
    if random.random() < 0.2:
        error_type = random.choice(["negative_weight", "missing_shipment_id", "invalid_status"])
        
        if error_type == "negative_weight":
            payload["weight_kg"] = -100.0
        elif error_type == "missing_shipment_id":
            payload["shipment_id"] = None
        elif error_type == "invalid_status":
            payload["status"] = "INVALID_STATUS"
    
    return payload

def generate_vehicle() -> dict:
    """Generate logistics vehicle reference/update (batch)"""
    payload = {
        "vehicle_id": str(uuid4()),
        "vehicle_type": random.choice(["TRUCK", "VAN", "CONTAINER_SHIP", "CARGO_PLANE"]),
        "capacity_kg": round(random.uniform(500, 20000), 2),
        "status": random.choice(["ACTIVE", "MAINTENANCE", "RETIRED", "INACTIVE"]),
        "updated_at": now_utc_iso(),
    }
    
    if random.random() < 0.2:
        error_type = random.choice(["missing_vehicle_id", "negative_capacity", "invalid_status"])
        
        if error_type == "missing_vehicle_id":
            payload["vehicle_id"] = None
        elif error_type == "negative_capacity":
            payload["capacity_kg"] = -500.0
        elif error_type == "invalid_status":
            payload["status"] = "UNKNOWN"
    
    return payload

def generate_location() -> dict:
    """Generate logistics location reference (batch)"""
    cities = [
        ("RBA", "Rabat", "Morocco", "CAPITAL"),
        ("CAS", "Casablanca", "Morocco", "INDUSTRIAL"),
        ("TNG", "Tangier", "Morocco", "PORT"),
        ("MRK", "Marrakech", "Morocco", "TOURIST"),
        ("FEZ", "Fez", "Morocco", "HISTORICAL"),
        ("BGR", "Benguerir", "Morocco", "EDUCATION"),
    ]
    
    loc_id, city, country, zone_type = random.choice(cities)
    
    payload = {
        "location_id": loc_id,
        "city": city,
        "country": country,
        "zone_type": zone_type,
        "updated_at": now_utc_iso(),
    }
    
    if random.random() < 0.2:
        error_type = random.choice(["missing_location_id", "invalid_zone_type", "missing_city"])
        
        if error_type == "missing_location_id":
            payload["location_id"] = None
        elif error_type == "invalid_zone_type":
            payload["zone_type"] = "INVALID"
        elif error_type == "missing_city":
            payload["city"] = None
    
    return payload