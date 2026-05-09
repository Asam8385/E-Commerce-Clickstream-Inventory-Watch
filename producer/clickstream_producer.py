import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Configuration (overridable via environment variables)
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC: str = os.getenv("KAFKA_TOPIC", "clickstream-events")
EVENTS_PER_SECOND: float = float(os.getenv("EVENTS_PER_SECOND", "10"))

# Synthetic catalogue — 20 products across 4 categories
PRODUCTS: list[dict] = [
    {"product_id": f"PROD-{i:03d}", "category": cat}
    for i, cat in enumerate(
        ["Laptops"] * 5 + ["Smartphones"] * 5 + ["Accessories"] * 5 + ["TVs"] * 5,
        start=1,
    )
]

# Realistic event-type distribution: views dominate, purchases are rare
EVENT_TYPES: list[str] = (
    ["view"] * 70 + ["add_to_cart"] * 20 + ["purchase"] * 10
)

# Simulate ~500 distinct users
USER_IDS: list[str] = [f"USER-{str(uuid.uuid4())[:8].upper()}" for _ in range(500)]


def build_event() -> dict:
    product = random.choice(PRODUCTS)
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(USER_IDS),
        "product_id": product["product_id"],
        "category": product["category"],
        "event_type": random.choice(EVENT_TYPES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "session_id": str(uuid.uuid4()),
        "device": random.choice(["desktop", "mobile", "tablet"]),
        "referrer": random.choice(["organic", "paid_search", "email", "social", "direct"]),
    }


def on_send_success(record_metadata) -> None:
    pass  # high-throughput path — suppress per-message logging


def on_send_error(exc: KafkaError) -> None:
    print(f"[ERROR] Failed to deliver message: {exc}", flush=True)


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=5,
        batch_size=16384,
    )

    interval: float = 1.0 / EVENTS_PER_SECOND
    produced: int = 0

    print(
        f"[INFO] Producer started — topic={TOPIC}  "
        f"rate={EVENTS_PER_SECOND} events/s  "
        f"brokers={BOOTSTRAP_SERVERS}",
        flush=True,
    )

    try:
        while True:
            event = build_event()
            producer.send(TOPIC, value=event).add_errback(on_send_error)
            produced += 1

            if produced % 1000 == 0:
                print(f"[INFO] {produced} events produced.", flush=True)

            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\n[INFO] Shutting down. Total events produced: {produced}", flush=True)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
