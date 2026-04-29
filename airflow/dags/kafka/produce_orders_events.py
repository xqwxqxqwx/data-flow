from __future__ import annotations

import argparse
import json
import os
import random
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
    event_ts = now - timedelta(minutes=random.randint(0, 120))
    return {
        "event_id": event_id,
        "user_id": random.randint(100, 999),
        "order_ts": event_ts.isoformat(),
        "amount": round(random.uniform(5.0, 350.0), 2),
        "currency": random.choice(currencies),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    parser.add_argument("--count", type=int, default=30)
    args = parser.parse_args()

    bootstrap = env("KAFKA_BOOTSTRAP", "kafka:9092")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda data: json.dumps(data).encode("utf-8"),
        linger_ms=50,
    )

    for i in range(1, args.count + 1):
        producer.send(args.topic, build_event(i))

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
