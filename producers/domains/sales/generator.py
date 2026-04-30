import random
from datetime import datetime, timezone
from uuid import uuid4


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_sales_transaction() -> dict:
    """
    Domain-specific mock event.
    This simulates a sales system
    """

    payload = {
        "transaction_id": str(uuid4()),
        "customer_id": str(uuid4()),
        "store_id": random.choice(["STORE_RABAT", "STORE_CASA", "STORE_BENGUERIR"]),
        "amount": round(random.uniform(20, 2000), 2),
        "currency": random.choice(["MAD", "EUR", "USD"]),
        "payment_method": random.choice(["CARD", "CASH", "TRANSFER"]),
        "transaction_time": now_utc_iso(),
    }

    # Intentionally inject bad data sometimes to test governance.
    if random.random() < 0.2:
        error_type = random.choice(["negative_amount", "bad_currency", "missing_transaction_id"])

        if error_type == "negative_amount":
            payload["amount"] = -50.0

        elif error_type == "bad_currency":
            payload["currency"] = "INVALID"

        elif error_type == "missing_transaction_id":
            payload["transaction_id"] = None

    return payload

def generate_sales_customer() -> dict:
    payload = {
        "customer_id": str(uuid4()),
        "full_name": random.choice([
            "Sara Bennani",
            "Yassine El Amrani",
            "Nora Ait Lahcen",
            "Mehdi Karimi"
        ]),
        "email": random.choice([
            "sara@example.com",
            "yassine@example.com",
            "nora@example.com",
            "mehdi@example.com"
        ]),
        "city": random.choice(["Rabat", "Casablanca", "Benguerir", "Marrakech"]),
        "customer_segment": random.choice(["RETAIL", "PREMIUM", "ENTERPRISE"]),
        "registration_time": now_utc_iso(),
    }

    if random.random() < 0.2:
        error_type = random.choice(["missing_customer_id", "bad_segment", "missing_email"])

        if error_type == "missing_customer_id":
            payload["customer_id"] = None
        elif error_type == "bad_segment":
            payload["customer_segment"] = "UNKNOWN"
        elif error_type == "missing_email":
            payload["email"] = None

    return payload

def generate_sales_product() -> dict:
    payload = {
        "product_id": str(uuid4()),
        "product_name": random.choice([
            "Laptop",
            "Smartphone",
            "Monitor",
            "Keyboard"
        ]),
        "category": random.choice(["ELECTRONICS", "ACCESSORIES", "COMPUTING"]),
        "price": round(random.uniform(50, 20000), 2),
        "currency": random.choice(["MAD", "EUR", "USD"]),
        "updated_at": now_utc_iso(),
    }

    if random.random() < 0.2:
        error_type = random.choice(["missing_product_id", "negative_price", "bad_currency"])

        if error_type == "missing_product_id":
            payload["product_id"] = None
        elif error_type == "negative_price":
            payload["price"] = -100.0
        elif error_type == "bad_currency":
            payload["currency"] = "INVALID"

    return payload