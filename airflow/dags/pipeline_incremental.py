from __future__ import annotations

import os
from datetime import datetime, timezone

import clickhouse_connect
import psycopg2
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from telegram_alert import airflow_telegram_failure_alert


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v


WATERMARK_VAR = "orders_incremental_watermark_ts"


def get_watermark_ts() -> str:
    return Variable.get(WATERMARK_VAR, default_var="1970-01-01T00:00:00+00:00")


def load_clickhouse_incremental(**context) -> None:
    watermark = get_watermark_ts()

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

    with psycopg2.connect(
        host=pg_host, port=pg_port, dbname=pg_db, user=pg_user, password=pg_password
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT order_id, user_id, order_ts, amount, currency
                FROM raw.orders
                WHERE order_ts > %s::timestamp
                ORDER BY order_ts, order_id
                """,
                (watermark,),
            )
            rows = cur.fetchall()

    if not rows:
        # No new data, do not move watermark
        return

    client = clickhouse_connect.get_client(
        host=ch_host, port=ch_port, username=ch_user, password=ch_password, database=ch_db
    )
    client.command(
        """
        CREATE TABLE IF NOT EXISTS raw_orders_inc
        (
          order_id UInt64,
          user_id UInt64,
          order_ts DateTime,
          amount Decimal(12, 2),
          currency String
        )
        ENGINE = ReplacingMergeTree(order_ts)
        ORDER BY (order_id)
        """
    )
    client.insert(
        "raw_orders_inc",
        rows,
        column_names=["order_id", "user_id", "order_ts", "amount", "currency"],
    )

    max_ts = max(r[2] for r in rows)
    if max_ts.tzinfo is None:
        max_ts = max_ts.replace(tzinfo=timezone.utc)
    Variable.set(WATERMARK_VAR, max_ts.isoformat())


with DAG(
    dag_id="data_flow_incremental",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data-flow",
        "on_failure_callback": airflow_telegram_failure_alert,
    },
    tags=["spark", "incremental"],
) as dag:
    spark_postgres_to_iceberg_incremental = BashOperator(
        task_id="spark_postgres_to_iceberg_incremental",
        bash_command=(
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark-apps/jobs/postgres_to_iceberg.py"
        ),
        env={
            "PATH": _env("PATH"),
            "JAVA_HOME": _env("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64"),
            "SOURCE_PG_HOST": _env("SOURCE_PG_HOST", "postgres-source"),
            "SOURCE_PG_PORT": _env("SOURCE_PG_PORT", "5432"),
            "SOURCE_PG_DB": _env("SOURCE_PG_DB", "source"),
            "SOURCE_PG_USER": _env("SOURCE_PG_USER", "source"),
            "SOURCE_PG_PASSWORD": _env("SOURCE_PG_PASSWORD", "source"),
            "INCREMENTAL_FROM_TS": get_watermark_ts(),
            "TARGET_ICEBERG_TABLE": "local.raw.orders_inc",
        },
    )

    load_to_clickhouse_incremental = PythonOperator(
        task_id="load_to_clickhouse_incremental",
        python_callable=load_clickhouse_incremental,
        provide_context=True,
    )

    dbt_run = BashOperator(
        task_id="dbt_run_incremental_models",
        bash_command=(
            "cd /opt/dbt && "
            "/opt/dbt_venv/bin/dbt run --profiles-dir /opt/dbt --select +orders_daily_inc"
        ),
        env={
            "DBT_CLICKHOUSE_HOST": _env("CLICKHOUSE_HOST", "clickhouse"),
            "DBT_CLICKHOUSE_PORT": _env("CLICKHOUSE_PORT", "8123"),
            "DBT_CLICKHOUSE_DB": _env("CLICKHOUSE_DB", "analytics"),
            "DBT_CLICKHOUSE_USER": _env("CLICKHOUSE_USER", "analytics"),
            "DBT_CLICKHOUSE_PASSWORD": _env("CLICKHOUSE_PASSWORD", "analytics"),
        },
    )

    dbt_test = BashOperator(
        task_id="dbt_test_incremental_models",
        bash_command=(
            "cd /opt/dbt && "
            "/opt/dbt_venv/bin/dbt test --profiles-dir /opt/dbt --select raw_orders_inc orders_daily_inc"
        ),
        env={
            "DBT_CLICKHOUSE_HOST": _env("CLICKHOUSE_HOST", "clickhouse"),
            "DBT_CLICKHOUSE_PORT": _env("CLICKHOUSE_PORT", "8123"),
            "DBT_CLICKHOUSE_DB": _env("CLICKHOUSE_DB", "analytics"),
            "DBT_CLICKHOUSE_USER": _env("CLICKHOUSE_USER", "analytics"),
            "DBT_CLICKHOUSE_PASSWORD": _env("CLICKHOUSE_PASSWORD", "analytics"),
        },
    )

    spark_postgres_to_iceberg_incremental >> load_to_clickhouse_incremental >> dbt_run >> dbt_test

