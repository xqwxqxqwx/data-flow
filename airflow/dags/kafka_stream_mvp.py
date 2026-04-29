from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v


with DAG(
    dag_id="kafka_stream_mvp",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-flow"},
    tags=["kafka", "mvp"],
) as dag:
    produce_events = BashOperator(
        task_id="produce_kafka_events",
        bash_command=(
            "python /opt/airflow/dags/kafka/produce_orders_events.py "
            "--topic orders_events "
            "--count 30"
        ),
        env={"KAFKA_BOOTSTRAP": _env("KAFKA_BOOTSTRAP", "kafka:9092")},
    )

    consume_to_clickhouse = BashOperator(
        task_id="consume_kafka_to_clickhouse",
        bash_command=(
            "python /opt/airflow/dags/kafka/consume_orders_to_clickhouse.py "
            "--topic orders_events "
            "--expected-count 30 "
            "--timeout-sec 30"
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
        task_id="dbt_run_kafka_models",
        bash_command=(
            "cd /opt/dbt && "
            "/opt/dbt_venv/bin/dbt run --profiles-dir /opt/dbt --select kafka_orders_daily"
        ),
        env={
            "DBT_CLICKHOUSE_HOST": _env("CLICKHOUSE_HOST", "clickhouse"),
            "DBT_CLICKHOUSE_PORT": _env("CLICKHOUSE_PORT", "8123"),
            "DBT_CLICKHOUSE_DB": _env("CLICKHOUSE_DB", "analytics"),
            "DBT_CLICKHOUSE_USER": _env("CLICKHOUSE_USER", "analytics"),
            "DBT_CLICKHOUSE_PASSWORD": _env("CLICKHOUSE_PASSWORD", "analytics"),
        },
    )

    produce_events >> consume_to_clickhouse >> dbt_run
