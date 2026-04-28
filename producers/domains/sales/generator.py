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