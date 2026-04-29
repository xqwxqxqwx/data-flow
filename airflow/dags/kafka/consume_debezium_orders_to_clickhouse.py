from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal

import clickhouse_connect
from kafka import KafkaConsumer


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing env var: {name}")
    return value


def _parse_ts_ms(ts_ms: int | None) -> datetime:
    if ts_ms is None:
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


def _to_decimal(v: object | None) -> Decimal | None:
    if v is None:
        return None
    return Decimal(str(v))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    parser.add_argument("--timeout-sec", type=int, default=20)
    parser.add_argument("--max-messages", type=int, default=5000)
    args = parser.parse_args()

    client = clickhouse_connect.get_client(
        host=env("CLICKHOUSE_HOST", "clickhouse"),
        port=int(env("CLICKHOUSE_PORT", "8123")),
        username=env("CLICKHOUSE_USER", "analytics"),
        password=env("CLICKHOUSE_PASSWORD", "analytics"),
        database=env("CLICKHOUSE_DB", "analytics"),
    )
    client.command(
        """
        CREATE TABLE IF NOT EXISTS cdc_orders_events
        (
          order_id UInt64,
          user_id Nullable(UInt64),
          order_ts Nullable(DateTime),
          amount Nullable(Decimal(12, 2)),
          currency Nullable(String),
          op Nullable(String),
          ts_ms Nullable(UInt64),
          ingested_at DateTime
        )
        ENGINE = MergeTree
        ORDER BY (order_id, ingested_at)
        """
    )

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
    while time.time() < deadline and len(rows) < args.max_messages:
        for msg in consumer:
            value = msg.value or {}
            payload = value.get("payload") or {}
            op = payload.get("op")
            ts_ms = payload.get("ts_ms")
            after = payload.get("after")
            before = payload.get("before")

            record = after if after is not None else before
            if record is None:
                continue

            order_id = int(record["order_id"])
            user_id = int(record["user_id"]) if record.get("user_id") is not None else None

            order_ts_raw = record.get("order_ts")
            order_ts = None
            if isinstance(order_ts_raw, str) and order_ts_raw:
                order_ts = datetime.fromisoformat(order_ts_raw.replace("Z", "+00:00"))

            amount = _to_decimal(record.get("amount"))
            currency = record.get("currency")

            rows.append(
                (
                    order_id,
                    user_id,
                    order_ts,
                    amount,
                    str(currency) if currency is not None else None,
                    str(op) if op is not None else None,
                    int(ts_ms) if ts_ms is not None else None,
                    _parse_ts_ms(int(ts_ms) if ts_ms is not None else None),
                )
            )
            if len(rows) >= args.max_messages:
                break

    consumer.close()

    if not rows:
        # Debezium is optional in this repo; allow empty consumption for convenience.
        return
    client.insert(
        "cdc_orders_events",
        rows,
        column_names=[
            "order_id",
            "user_id",
            "order_ts",
            "amount",
            "currency",
            "op",
            "ts_ms",
            "ingested_at",
        ],
    )


if __name__ == "__main__":
    main()

