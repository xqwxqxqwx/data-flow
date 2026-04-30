from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone

import boto3
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
    parser.add_argument("--timeout-sec", type=int, default=45)
    parser.add_argument("--prefix", default="bronze/orders")
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

    events: list[dict] = []
    deadline = time.time() + args.timeout_sec
    while time.time() < deadline and len(events) < args.expected_count:
        for msg in consumer:
            payload = msg.value
            payload["_kafka_topic"] = msg.topic
            payload["_kafka_partition"] = msg.partition
            payload["_kafka_offset"] = msg.offset
            payload["_ingest_ts"] = datetime.now(timezone.utc).isoformat()
            events.append(payload)
            if len(events) >= args.expected_count:
                break
    consumer.close()

    if not events:
        raise RuntimeError("No messages consumed from Kafka topic.")

    run_ts = datetime.now(timezone.utc)
    run_key = run_ts.strftime("%Y%m%dT%H%M%SZ")
    date_key = run_ts.strftime("%Y-%m-%d")
    object_key = f"{args.prefix}/dt={date_key}/run={run_key}.jsonl"
    body = "\n".join(json.dumps(x, ensure_ascii=True) for x in events).encode("utf-8")

    s3 = boto3.client(
        "s3",
        endpoint_url=env("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=env("MINIO_ACCESS_KEY", "minio"),
        aws_secret_access_key=env("MINIO_SECRET_KEY", "minio12345"),
    )
    s3.put_object(
        Bucket=env("MINIO_RAW_BUCKET", "raw"),
        Key=object_key,
        Body=body,
        ContentType="application/json",
    )


if __name__ == "__main__":
    main()
