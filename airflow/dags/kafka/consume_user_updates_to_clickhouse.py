from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime

import clickhouse_connect
from kafka import KafkaConsumer


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing env var: {name}")
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    parser.add_argument("--expected-count", type=int, default=50)
    parser.add_argument("--timeout-sec", type=int, default=30)
    args = parser.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=env("KAFKA_BOOTSTRAP", "kafka:9092"),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=1000,
    )

    rows: list[tuple] = []
    deadline = time.time() + args.timeout_sec
    while time.time() < deadline and len(rows) < args.expected_count:
        for msg in consumer:
            p = msg.value
            rows.append(
                (
                    int(p["update_id"]),
                    int(p["user_id"]),
                    datetime.fromisoformat(p["updated_at"].replace("Z", "+00:00")),
                    str(p["email"]),
                    str(p["country"]),
                )
            )
            if len(rows) >= args.expected_count:
                break
    consumer.close()

    if not rows:
        raise RuntimeError("No messages consumed from Kafka topic.")

    client = clickhouse_connect.get_client(
        host=env("CLICKHOUSE_HOST", "clickhouse"),
        port=int(env("CLICKHOUSE_PORT", "8123")),
        username=env("CLICKHOUSE_USER", "analytics"),
        password=env("CLICKHOUSE_PASSWORD", "analytics"),
        database=env("CLICKHOUSE_DB", "analytics"),
    )
    client.command(
        """
        CREATE TABLE IF NOT EXISTS user_updates_raw
        (
          update_id UInt64,
          user_id UInt64,
          updated_at DateTime,
          email String,
          country String
        )
        ENGINE = MergeTree
        ORDER BY (user_id, updated_at, update_id)
        """
    )
    client.insert(
        "user_updates_raw",
        rows,
        column_names=["update_id", "user_id", "updated_at", "email", "country"],
    )


if __name__ == "__main__":
    main()

