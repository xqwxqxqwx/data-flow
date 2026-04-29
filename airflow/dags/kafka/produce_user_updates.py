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


COUNTRIES = ["RU", "DE", "US", "TR", "KZ"]


def build_event(update_id: int, user_id: int) -> dict:
    now = datetime.now(timezone.utc)
    ts = now - timedelta(minutes=random.randint(0, 240))
    email = f"user{user_id}@example.com"
    # simulate changes
    if random.random() < 0.4:
        email = f"user{user_id}+{update_id}@example.com"
    return {
        "update_id": update_id,
        "user_id": user_id,
        "updated_at": ts.isoformat(),
        "email": email,
        "country": random.choice(COUNTRIES),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    parser.add_argument("--count", type=int, default=50)
    parser.add_argument("--users", type=int, default=10)
    args = parser.parse_args()

    bootstrap = env("KAFKA_BOOTSTRAP", "kafka:9092")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda data: json.dumps(data).encode("utf-8"),
        linger_ms=50,
    )

    for i in range(1, args.count + 1):
        user_id = random.randint(1, args.users)
        producer.send(args.topic, build_event(i, user_id))

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()

