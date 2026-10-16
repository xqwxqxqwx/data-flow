from __future__ import annotations

import argparse
import base64
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
    if isinstance(v, dict):
        # Kafka Connect JSON decimal payload can arrive as {"scale": 2, "value": "base64..."}
        scale = int(v.get("scale", 0))
        encoded = v.get("value")
        if isinstance(encoded, str) and encoded:
            raw = base64.b64decode(encoded)
            unscaled = int.from_bytes(raw, byteorder="big", signed=True)
            return Decimal(unscaled) / (Decimal(10) ** scale)
    if isinstance(v, str):
        try:
            return Decimal(v)
        except Exception:
            # Debezium can emit Decimal as base64-encoded bytes for JSON converter.
            raw = base64.b64decode(v)
            unscaled = int.from_bytes(raw, byteorder="big", signed=True)
            return Decimal(unscaled) / Decimal(100)
    return Decimal(str(v))


def _parse_order_ts(raw: object | None) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, str) and raw:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if isinstance(raw, (int, float)):
        value = int(raw)
        # Debezium can emit epoch in microseconds/milliseconds/seconds depending on column mode.
        if value > 10_000_000_000_000:
            return datetime.fromtimestamp(value / 1_000_000.0, tz=timezone.utc)
        if value > 10_000_000_000:
            return datetime.fromtimestamp(value / 1_000.0, tz=timezone.utc)
        return datetime.fromtimestamp(value, tz=timezone.utc)
    return None


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

            order_ts = _parse_order_ts(record.get("order_ts"))

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
        raise RuntimeError("No CDC messages consumed from Debezium topic.")
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

