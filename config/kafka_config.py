import os

# ---------------------------------------------------------------------------
# Kafka connection constants
# All values can be overridden via environment variables so that the same
# module is usable both inside Docker (kafka:29092) and locally (localhost:9092).
# ---------------------------------------------------------------------------

BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_CLICKSTREAM: str = os.getenv("KAFKA_TOPIC", "clickstream-events")

# Consumer group IDs
CONSUMER_GROUP_SPARK: str = "spark-streaming-consumer"
CONSUMER_GROUP_REPORT: str = "report-consumer"

# Producer settings
PRODUCER_CONFIG: dict = {
    "bootstrap_servers": BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 3,
    "linger_ms": 5,
    "batch_size": 16384,
}

# Consumer base settings (merge with group-specific overrides as needed)
CONSUMER_BASE_CONFIG: dict = {
    "bootstrap_servers": BOOTSTRAP_SERVERS,
    "auto_offset_reset": "earliest",
    "enable_auto_commit": False,
    "value_deserializer": None,   # set at instantiation
}
