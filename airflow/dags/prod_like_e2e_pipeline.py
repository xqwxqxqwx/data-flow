from __future__ import annotations

import os
from datetime import datetime

import clickhouse_connect
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from telegram_alert import airflow_telegram_failure_alert


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v


def prepare_clickhouse_bronze_stream() -> None:
    ch_host = _env("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(_env("CLICKHOUSE_PORT", "8123"))
    ch_db = _env("CLICKHOUSE_DB", "analytics")
    ch_user = _env("CLICKHOUSE_USER", "analytics")
    ch_password = _env("CLICKHOUSE_PASSWORD", "analytics")

    client = clickhouse_connect.get_client(
        host=ch_host, port=ch_port, username=ch_user, password=ch_password, database=ch_db
    )
    client.command(
        """
        CREATE TABLE IF NOT EXISTS bronze_orders_stream
        (
          event_id UInt64,
          user_id UInt64,
          order_ts DateTime,
          amount Decimal(12, 2),
          currency String,
          ingest_ts DateTime
        )
        ENGINE = ReplacingMergeTree(ingest_ts)
        ORDER BY (event_id)
        """
    )

    client.command(
        """
        INSERT INTO bronze_orders_stream
        SELECT
          event_id,
          user_id,
          order_ts,
          amount,
          currency,
          now() as ingest_ts
        FROM kafka_orders_raw
        """
    )


with DAG(
    dag_id="prod_like_e2e_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data-flow",
        "on_failure_callback": airflow_telegram_failure_alert,
    },
    tags=["prod-like", "kafka", "spark", "dbt", "layered"],
) as dag:
    produce_events = BashOperator(
        task_id="produce_orders_events",
        bash_command=(
            "python /opt/airflow/dags/kafka/produce_orders_events.py "
            "--topic orders_events_prod "
            "--count 60"
        ),
        env={"KAFKA_BOOTSTRAP": _env("KAFKA_BOOTSTRAP", "kafka:9092")},
    )

    consume_to_bronze = BashOperator(
        task_id="consume_orders_to_minio_bronze",
        bash_command=(
            "python /opt/airflow/dags/kafka/consume_orders_to_minio_bronze.py "
            "--topic orders_events_prod "
            "--expected-count 60 "
            "--timeout-sec 45 "
            "--prefix bronze/orders_prod"
        ),
        env={
            "KAFKA_BOOTSTRAP": _env("KAFKA_BOOTSTRAP", "kafka:9092"),
            "MINIO_ENDPOINT": _env("MINIO_ENDPOINT", "http://minio:9000"),
            "MINIO_ACCESS_KEY": _env("MINIO_ACCESS_KEY", "minio"),
            "MINIO_SECRET_KEY": _env("MINIO_SECRET_KEY", "minio12345"),
            "MINIO_RAW_BUCKET": _env("MINIO_RAW_BUCKET", "raw"),
        },
    )

    spark_bronze_to_silver = SparkSubmitOperator(
        task_id="spark_bronze_to_iceberg_silver",
        application="/opt/spark-apps/jobs/bronze_orders_to_iceberg_silver.py",
        conn_id="spark_default",
        conf={
            "spark.executor.cores": "2",
            "spark.cores.max": "3",
            "spark.sql.shuffle.partitions": "6",
        },
        env_vars={
            "MINIO_RAW_BUCKET": _env("MINIO_RAW_BUCKET", "raw"),
            "BRONZE_INPUT_PREFIX": "bronze/orders_prod",
            "TARGET_ICEBERG_TABLE": "local.silver.orders_stream",
        },
    )

    load_to_clickhouse = BashOperator(
        task_id="consume_kafka_to_clickhouse_bronze",
        bash_command=(
            "python /opt/airflow/dags/kafka/consume_orders_to_clickhouse.py "
            "--topic orders_events_prod "
            "--expected-count 60 "
            "--timeout-sec 45"
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

    prepare_clickhouse_bronze = PythonOperator(
        task_id="prepare_clickhouse_bronze_stream",
        python_callable=prepare_clickhouse_bronze_stream,
    )

    dbt_run_layered = BashOperator(
        task_id="dbt_run_layered_models",
        bash_command=(
            "cd /opt/dbt && "
            "/opt/dbt_venv/bin/dbt run --profiles-dir /opt/dbt "
            "--select bronze_orders_stream silver_orders_stream gold_orders_daily"
        ),
        env={
            "DBT_CLICKHOUSE_HOST": _env("CLICKHOUSE_HOST", "clickhouse"),
            "DBT_CLICKHOUSE_PORT": _env("CLICKHOUSE_PORT", "8123"),
            "DBT_CLICKHOUSE_DB": _env("CLICKHOUSE_DB", "analytics"),
            "DBT_CLICKHOUSE_USER": _env("CLICKHOUSE_USER", "analytics"),
            "DBT_CLICKHOUSE_PASSWORD": _env("CLICKHOUSE_PASSWORD", "analytics"),
        },
    )

    dbt_test_layered = BashOperator(
        task_id="dbt_test_layered_models",
        bash_command=(
            "cd /opt/dbt && "
            "/opt/dbt_venv/bin/dbt test --profiles-dir /opt/dbt "
            "--select bronze_orders_stream silver_orders_stream gold_orders_daily"
        ),
        env={
            "DBT_CLICKHOUSE_HOST": _env("CLICKHOUSE_HOST", "clickhouse"),
            "DBT_CLICKHOUSE_PORT": _env("CLICKHOUSE_PORT", "8123"),
            "DBT_CLICKHOUSE_DB": _env("CLICKHOUSE_DB", "analytics"),
            "DBT_CLICKHOUSE_USER": _env("CLICKHOUSE_USER", "analytics"),
            "DBT_CLICKHOUSE_PASSWORD": _env("CLICKHOUSE_PASSWORD", "analytics"),
        },
    )

    (
        produce_events
        >> [consume_to_bronze, load_to_clickhouse]
        >> spark_bronze_to_silver
        >> prepare_clickhouse_bronze
        >> dbt_run_layered
        >> dbt_test_layered
    )
