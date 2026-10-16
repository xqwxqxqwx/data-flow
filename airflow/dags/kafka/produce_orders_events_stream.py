from __future__ import annotations

import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing env var: {name}")
    return value


def build_event(event_id: int) -> dict:
    currencies = ["USD", "EUR", "RUB"]
    now = datetime.now(timezone.utc)
    event_ts = now - timedelta(seconds=random.randint(0, 60))
    return {
        "event_id": event_id,
        "user_id": random.randint(100, 999),
        "order_ts": event_ts.isoformat(),
        "amount": round(random.uniform(5.0, 350.0), 2),
        "currency": random.choice(currencies),
    }


def main() -> None:
    bootstrap = env("KAFKA_BOOTSTRAP", "kafka:9092")
    topic = env("KAFKA_STREAM_TOPIC", "orders_events_stream")
    interval_sec = float(env("PRODUCER_INTERVAL_SEC", "0.5"))

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda data: json.dumps(data).encode("utf-8"),
        linger_ms=50,
    )
    event_id = 1
    while True:
        producer.send(topic, build_event(event_id))
        producer.flush()
        event_id += 1
        time.sleep(interval_sec)


if __name__ == "__main__":
    main()
