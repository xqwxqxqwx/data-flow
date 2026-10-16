"""Relic Route batch: Postgres relic.* → ClickHouse bronze → dbt staging/mart."""

from __future__ import annotations

import os
from datetime import datetime
from decimal import Decimal
from typing import Any, Sequence

import clickhouse_connect
import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

try:
    from telegram_alert import airflow_telegram_failure_alert
except ImportError:  # local/unit without plugin
    airflow_telegram_failure_alert = None


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v


# Postgres SELECT → ClickHouse bronze table (full refresh)
TABLES: list[dict[str, Any]] = [
    {
        "pg_sql": """
            SELECT customer_id, email, full_name, city, created_at
            FROM relic.customers
            ORDER BY customer_id
        """,
        "ch_table": "bronze_relic_customers",
        "ch_ddl": """
            CREATE TABLE IF NOT EXISTS bronze_relic_customers
            (
              customer_id UInt64,
              email String,
              full_name String,
              city String,
              created_at DateTime
            )
            ENGINE = MergeTree
            ORDER BY customer_id
        """,
        "columns": ["customer_id", "email", "full_name", "city", "created_at"],
    },
    {
        "pg_sql": """
            SELECT item_id, title, category, brand, grade, status,
                   list_price, listed_at, updated_at
            FROM relic.items
            ORDER BY item_id
        """,
        "ch_table": "bronze_relic_items",
        "ch_ddl": """
            CREATE TABLE IF NOT EXISTS bronze_relic_items
            (
              item_id UInt64,
              title String,
              category LowCardinality(String),
              brand String,
              grade LowCardinality(String),
              status LowCardinality(String),
              list_price Decimal(12, 2),
              listed_at Nullable(DateTime),
              updated_at DateTime
            )
            ENGINE = MergeTree
            ORDER BY item_id
        """,
        "columns": [
            "item_id",
            "title",
            "category",
            "brand",
            "grade",
            "status",
            "list_price",
            "listed_at",
            "updated_at",
        ],
    },
    {
        "pg_sql": """
            SELECT van_id, name, van_type
            FROM relic.vans
            ORDER BY van_id
        """,
        "ch_table": "bronze_relic_vans",
        "ch_ddl": """
            CREATE TABLE IF NOT EXISTS bronze_relic_vans
            (
              van_id UInt64,
              name String,
              van_type LowCardinality(String)
            )
            ENGINE = MergeTree
            ORDER BY van_id
        """,
        "columns": ["van_id", "name", "van_type"],
    },
    {
        "pg_sql": """
            SELECT location_id, name, district, lat, lon
            FROM relic.stop_locations
            ORDER BY location_id
        """,
        "ch_table": "bronze_relic_stop_locations",
        "ch_ddl": """
            CREATE TABLE IF NOT EXISTS bronze_relic_stop_locations
            (
              location_id UInt64,
              name String,
              district LowCardinality(String),
              lat Nullable(Float64),
              lon Nullable(Float64)
            )
            ENGINE = MergeTree
            ORDER BY location_id
        """,
        "columns": ["location_id", "name", "district", "lat", "lon"],
    },
    {
        "pg_sql": """
            SELECT stop_id, van_id, location_id, stop_date, started_at, ended_at
            FROM relic.van_stops
            ORDER BY stop_id
        """,
        "ch_table": "bronze_relic_van_stops",
        "ch_ddl": """
            CREATE TABLE IF NOT EXISTS bronze_relic_van_stops
            (
              stop_id UInt64,
              van_id UInt64,
              location_id UInt64,
              stop_date Date,
              started_at DateTime,
              ended_at Nullable(DateTime)
            )
            ENGINE = MergeTree
            ORDER BY (stop_date, stop_id)
        """,
        "columns": [
            "stop_id",
            "van_id",
            "location_id",
            "stop_date",
            "started_at",
            "ended_at",
        ],
    },
    {
        "pg_sql": """
            SELECT order_id, customer_id, order_ts, channel, status, currency,
                   total_amount, van_id, stop_id, updated_at
            FROM relic.orders
            ORDER BY order_id
        """,
        "ch_table": "bronze_relic_orders",
        "ch_ddl": """
            CREATE TABLE IF NOT EXISTS bronze_relic_orders
            (
              order_id UInt64,
              customer_id UInt64,
              order_ts DateTime,
              channel LowCardinality(String),
              status LowCardinality(String),
              currency LowCardinality(String),
              total_amount Decimal(12, 2),
              van_id Nullable(UInt64),
              stop_id Nullable(UInt64),
              updated_at DateTime
            )
            ENGINE = MergeTree
            ORDER BY (order_ts, order_id)
        """,
        "columns": [
            "order_id",
            "customer_id",
            "order_ts",
            "channel",
            "status",
            "currency",
            "total_amount",
            "van_id",
            "stop_id",
            "updated_at",
        ],
    },
    {
        "pg_sql": """
            SELECT order_id, item_id, unit_price, qty
            FROM relic.order_items
            ORDER BY order_id, item_id
        """,
        "ch_table": "bronze_relic_order_items",
        "ch_ddl": """
            CREATE TABLE IF NOT EXISTS bronze_relic_order_items
            (
              order_id UInt64,
              item_id UInt64,
              unit_price Decimal(12, 2),
              qty UInt32
            )
            ENGINE = MergeTree
            ORDER BY (order_id, item_id)
        """,
        "columns": ["order_id", "item_id", "unit_price", "qty"],
    },
    {
        "pg_sql": """
            SELECT delivery_id, order_id, method, city, address, status, delivery_fee,
                   created_at, shipped_at, delivered_at, updated_at
            FROM relic.deliveries
            ORDER BY delivery_id
        """,
        "ch_table": "bronze_relic_deliveries",
        "ch_ddl": """
            CREATE TABLE IF NOT EXISTS bronze_relic_deliveries
            (
              delivery_id UInt64,
              order_id UInt64,
              method LowCardinality(String),
              city LowCardinality(String),
              address String,
              status LowCardinality(String),
              delivery_fee Decimal(12, 2),
              created_at DateTime,
              shipped_at Nullable(DateTime),
              delivered_at Nullable(DateTime),
              updated_at DateTime
            )
            ENGINE = MergeTree
            ORDER BY delivery_id
        """,
        "columns": [
            "delivery_id",
            "order_id",
            "method",
            "city",
            "address",
            "status",
            "delivery_fee",
            "created_at",
            "shipped_at",
            "delivered_at",
            "updated_at",
        ],
    },
]


def _normalize_row(row: Sequence[Any]) -> list[Any]:
    out: list[Any] = []
    for v in row:
        if isinstance(v, Decimal):
            out.append(v)
        elif hasattr(v, "isoformat") and not isinstance(v, str):
            # date/datetime → leave as-is; clickhouse_connect accepts them
            out.append(v)
        else:
            out.append(v)
    return out


def load_relic_bronze() -> None:
    pg_host = _env("SOURCE_PG_HOST", "postgres-source")
    pg_port = int(_env("SOURCE_PG_PORT", "5432"))
    pg_db = _env("SOURCE_PG_DB", "source")
    pg_user = _env("SOURCE_PG_USER", "source")
    pg_password = _env("SOURCE_PG_PASSWORD", "source")

    ch_host = _env("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(_env("CLICKHOUSE_PORT", "8123"))
    ch_db = _env("CLICKHOUSE_DB", "analytics")
    ch_user = _env("CLICKHOUSE_USER", "analytics")
    ch_password = _env("CLICKHOUSE_PASSWORD", "analytics")

    client = clickhouse_connect.get_client(
        host=ch_host, port=ch_port, username=ch_user, password=ch_password, database=ch_db
    )

    with psycopg2.connect(
        host=pg_host, port=pg_port, dbname=pg_db, user=pg_user, password=pg_password
    ) as conn:
        with conn.cursor() as cur:
            for spec in TABLES:
                cur.execute(spec["pg_sql"])
                rows = [_normalize_row(r) for r in cur.fetchall()]
                # full refresh: recreate so source schema changes reach bronze
                client.command(f"DROP TABLE IF EXISTS {spec['ch_table']}")
                client.command(spec["ch_ddl"])
                if rows:
                    client.insert(spec["ch_table"], rows, column_names=spec["columns"])
                print(f"{spec['ch_table']}: {len(rows)} rows")


default_args: dict[str, Any] = {
    "owner": "data-flow",
}
if airflow_telegram_failure_alert is not None:
    default_args["on_failure_callback"] = airflow_telegram_failure_alert

with DAG(
    dag_id="relic_route_batch",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["relic", "batch", "mvp"],
) as dag:
    load_bronze = PythonOperator(
        task_id="pg_to_ch_bronze",
        python_callable=load_relic_bronze,
    )

    dbt_run = BashOperator(
        task_id="dbt_run_relic",
        bash_command=(
            "cd /opt/dbt && /opt/dbt_venv/bin/dbt run --profiles-dir /opt/dbt "
            "--select path:models/relic"
        ),
        env={
            "PATH": _env("PATH", "/usr/local/bin:/usr/bin:/bin"),
            "DBT_CLICKHOUSE_HOST": _env("CLICKHOUSE_HOST", "clickhouse"),
            "DBT_CLICKHOUSE_PORT": _env("CLICKHOUSE_PORT", "8123"),
            "DBT_CLICKHOUSE_DB": _env("CLICKHOUSE_DB", "analytics"),
            "DBT_CLICKHOUSE_USER": _env("CLICKHOUSE_USER", "analytics"),
            "DBT_CLICKHOUSE_PASSWORD": _env("CLICKHOUSE_PASSWORD", "analytics"),
        },
    )

    dbt_test = BashOperator(
        task_id="dbt_test_relic",
        bash_command=(
            "cd /opt/dbt && /opt/dbt_venv/bin/dbt test --profiles-dir /opt/dbt "
            "--select path:models/relic"
        ),
        env={
            "PATH": _env("PATH", "/usr/local/bin:/usr/bin:/bin"),
            "DBT_CLICKHOUSE_HOST": _env("CLICKHOUSE_HOST", "clickhouse"),
            "DBT_CLICKHOUSE_PORT": _env("CLICKHOUSE_PORT", "8123"),
            "DBT_CLICKHOUSE_DB": _env("CLICKHOUSE_DB", "analytics"),
            "DBT_CLICKHOUSE_USER": _env("CLICKHOUSE_USER", "analytics"),
            "DBT_CLICKHOUSE_PASSWORD": _env("CLICKHOUSE_PASSWORD", "analytics"),
        },
    )

    load_bronze >> dbt_run >> dbt_test
