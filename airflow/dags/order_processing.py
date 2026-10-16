from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
import socket
import time
from datetime import datetime
from telegram_alert import airflow_telegram_failure_alert

def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v



def asd():
    hook = PostgresHook(postgres_conn_id="postgres_source")
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM raw.orders
            LIMIT 10
            """
        )
        rows = cur.fetchall()
    print(rows)
    print(1)



with DAG(
    dag_id="pg_hook",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data-flow",
        "on_failure_callback": airflow_telegram_failure_alert,
    },
    tags=["pg"],
) as dag:
    conne = PythonOperator(
        task_id="asd",
        python_callable=asd,
    )

conne