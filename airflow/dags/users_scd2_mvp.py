from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from telegram_alert import airflow_telegram_failure_alert


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v


with DAG(
    dag_id="users_scd2_mvp",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data-flow",
        "on_failure_callback": airflow_telegram_failure_alert,
    },
    tags=["kafka", "dbt", "scd2", "mvp"],
) as dag:
    produce_updates = BashOperator(
        task_id="produce_user_updates",
        bash_command=(
            "python /opt/airflow/dags/kafka/produce_user_updates.py "
            "--topic user_updates "
            "--count 60 "
            "--users 12"
        ),
        env={"KAFKA_BOOTSTRAP": _env("KAFKA_BOOTSTRAP", "kafka:9092")},
    )

    consume_updates = BashOperator(
        task_id="consume_user_updates_to_clickhouse",
        bash_command=(
            "python /opt/airflow/dags/kafka/consume_user_updates_to_clickhouse.py "
            "--topic user_updates "
            "--expected-count 60 "
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
        task_id="dbt_run_users_scd2",
        bash_command=(
            "cd /opt/dbt && "
            "/opt/dbt_venv/bin/dbt run --profiles-dir /opt/dbt --select +dim_users_scd2"
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
        task_id="dbt_test_users_scd2",
        bash_command=(
            "cd /opt/dbt && "
            "/opt/dbt_venv/bin/dbt test --profiles-dir /opt/dbt --select dim_users_scd2"
        ),
        env={
            "DBT_CLICKHOUSE_HOST": _env("CLICKHOUSE_HOST", "clickhouse"),
            "DBT_CLICKHOUSE_PORT": _env("CLICKHOUSE_PORT", "8123"),
            "DBT_CLICKHOUSE_DB": _env("CLICKHOUSE_DB", "analytics"),
            "DBT_CLICKHOUSE_USER": _env("CLICKHOUSE_USER", "analytics"),
            "DBT_CLICKHOUSE_PASSWORD": _env("CLICKHOUSE_PASSWORD", "analytics"),
        },
    )

    produce_updates >> consume_updates >> dbt_run >> dbt_test

