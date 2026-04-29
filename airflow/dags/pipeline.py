from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import clickhouse_connect
import psycopg2

from telegram_alert import airflow_telegram_failure_alert


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v


with DAG(
    dag_id="data_flow_mvp",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data-flow",
        "on_failure_callback": airflow_telegram_failure_alert,
    },
    tags=["mvp"],
) as dag:
    spark_postgres_to_iceberg = BashOperator(
        task_id="spark_postgres_to_iceberg",
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
        },
    )

    def load_clickhouse() -> None:
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
                    ORDER BY order_id
                    """
                )
                rows = cur.fetchall()

        client = clickhouse_connect.get_client(
            host=ch_host, port=ch_port, username=ch_user, password=ch_password, database=ch_db
        )
        client.command(
            """
            CREATE TABLE IF NOT EXISTS raw_orders
            (
              order_id UInt64,
              user_id UInt64,
              order_ts DateTime,
              amount Decimal(12, 2),
              currency String
            )
            ENGINE = MergeTree
            ORDER BY (order_ts, order_id)
            """
        )
        client.command("TRUNCATE TABLE raw_orders")
        client.insert(
            "raw_orders",
            rows,
            column_names=["order_id", "user_id", "order_ts", "amount", "currency"],
        )

    load_to_clickhouse = PythonOperator(
        task_id="load_to_clickhouse",
        python_callable=load_clickhouse,
    )

    dbt_run = BashOperator(
        task_id="dbt_run_clickhouse",
        bash_command="cd /opt/dbt && /opt/dbt_venv/bin/dbt run --profiles-dir /opt/dbt",
        env={
            "DBT_CLICKHOUSE_HOST": _env("CLICKHOUSE_HOST", "clickhouse"),
            "DBT_CLICKHOUSE_PORT": _env("CLICKHOUSE_PORT", "8123"),
            "DBT_CLICKHOUSE_DB": _env("CLICKHOUSE_DB", "analytics"),
            "DBT_CLICKHOUSE_USER": _env("CLICKHOUSE_USER", "analytics"),
            "DBT_CLICKHOUSE_PASSWORD": _env("CLICKHOUSE_PASSWORD", "analytics"),
        },
    )

    dbt_test = BashOperator(
        task_id="dbt_test_clickhouse",
        bash_command="cd /opt/dbt && /opt/dbt_venv/bin/dbt test --profiles-dir /opt/dbt --select raw_orders orders_daily",
        env={
            "DBT_CLICKHOUSE_HOST": _env("CLICKHOUSE_HOST", "clickhouse"),
            "DBT_CLICKHOUSE_PORT": _env("CLICKHOUSE_PORT", "8123"),
            "DBT_CLICKHOUSE_DB": _env("CLICKHOUSE_DB", "analytics"),
            "DBT_CLICKHOUSE_USER": _env("CLICKHOUSE_USER", "analytics"),
            "DBT_CLICKHOUSE_PASSWORD": _env("CLICKHOUSE_PASSWORD", "analytics"),
        },
    )

    spark_postgres_to_iceberg >> load_to_clickhouse >> dbt_run >> dbt_test
