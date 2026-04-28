from datetime import datetime, timezone
from uuid import uuid4


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# standard event wrapper

def wrap_event(domain: str, event_type: str, payload: dict) -> dict:
    return {
        "event_id": str(uuid4()),
        "domain": domain,
        "event_type": event_type,
        "produced_at": now_utc_iso(),
        "payload": payload,
    }