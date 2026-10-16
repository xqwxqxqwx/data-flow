from __future__ import annotations

import os
import socket
import time
from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from telegram_alert import airflow_telegram_failure_alert


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v


def mutate_postgres_orders() -> None:
    pg_host = _env("SOURCE_PG_HOST", "postgres-source")
    pg_port = int(_env("SOURCE_PG_PORT", "5432"))
    pg_db = _env("SOURCE_PG_DB", "source")
    pg_user = _env("SOURCE_PG_USER", "source")
    pg_password = _env("SOURCE_PG_PASSWORD", "source")

    with psycopg2.connect(
        host=pg_host, port=pg_port, dbname=pg_db, user=pg_user, password=pg_password
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw.orders(order_id, user_id, order_ts, amount, currency)
                VALUES (1001, 777, NOW(), 55.10, 'USD')
                ON CONFLICT (order_id) DO UPDATE SET amount = EXCLUDED.amount;
                """
            )
            cur.execute("UPDATE raw.orders SET amount = amount + 1 WHERE order_id = 2;")
            cur.execute("DELETE FROM raw.orders WHERE order_id = 1;")
        conn.commit()


def require_debezium_connect() -> None:
    host = os.getenv("DEBEZIUM_HOST", "debezium")
    port = int(os.getenv("DEBEZIUM_PORT", "8083"))
    timeout_sec = int(os.getenv("DEBEZIUM_WAIT_TIMEOUT_SEC", "90"))
    deadline = time.time() + timeout_sec
    last_error: OSError | None = None

    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError as e:
            last_error = e
            time.sleep(2)

    raise RuntimeError(
        f"Debezium Connect not reachable at {host}:{port} after {timeout_sec}s: {last_error}"
    )


with DAG(
    dag_id="cdc_orders_mvp",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data-flow",
        "on_failure_callback": airflow_telegram_failure_alert,
    },
    tags=["cdc", "debezium", "mvp"],
) as dag:
    mutate_source = PythonOperator(
        task_id="mutate_postgres_orders",
        python_callable=mutate_postgres_orders,
    )

    check_debezium = PythonOperator(
        task_id="require_debezium_connect",
        python_callable=require_debezium_connect,
    )

    consume_cdc_to_clickhouse = BashOperator(
        task_id="consume_cdc_to_clickhouse",
        bash_command=(
            "python /opt/airflow/dags/kafka/consume_debezium_orders_to_clickhouse.py "
            "--topic source.raw.orders "
            "--timeout-sec 20 "
            "--max-messages 2000"
        ),
        env={
            "KAFKA_BOOTSTRAP": _env("KAFKA_BOOTSTRAP", "kafka:9092"),
            "CLICKHOUSE_HOST": _env("CLICKHOUSE_HOST", "clickhouse"),
            "CLICKHOUSE_PORT": _env("CLICKHOUSE_PORT", "8123"),
            "CLICKHOUSE_DB": _env("CLICKHOUSE_DB", "analytics"),
            "CLICKHOUSE_USER": _env("CLICKHOUSE_USER", "analytics"),
            "CLICKHOUSE_PASSWORD": _env("CLICKHOUSE_PASSWORD", "analytics"),
        },
    )

    dbt_run = BashOperator(
        task_id="dbt_run_cdc_models",
        bash_command=(
            "cd /opt/dbt && "
            "/opt/dbt_venv/bin/dbt run --profiles-dir /opt/dbt --select cdc_orders_current"
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
        task_id="dbt_test_cdc_models",
        bash_command=(
            "cd /opt/dbt && "
            "/opt/dbt_venv/bin/dbt test --profiles-dir /opt/dbt --select cdc_orders_current"
        ),
        env={
            "DBT_CLICKHOUSE_HOST": _env("CLICKHOUSE_HOST", "clickhouse"),
            "DBT_CLICKHOUSE_PORT": _env("CLICKHOUSE_PORT", "8123"),
            "DBT_CLICKHOUSE_DB": _env("CLICKHOUSE_DB", "analytics"),
            "DBT_CLICKHOUSE_USER": _env("CLICKHOUSE_USER", "analytics"),
            "DBT_CLICKHOUSE_PASSWORD": _env("CLICKHOUSE_PASSWORD", "analytics"),
        },
    )

    mutate_source >> check_debezium >> consume_cdc_to_clickhouse >> dbt_run >> dbt_test

