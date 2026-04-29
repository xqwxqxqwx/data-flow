from __future__ import annotations

import os
from typing import Any

import requests


def _env(name: str) -> str | None:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return None
    return v.strip()


def airflow_telegram_failure_alert(context: dict[str, Any]) -> None:
    """
    Airflow on_failure_callback.

    Opt-in via env vars:
      - TELEGRAM_BOT_TOKEN
      - TELEGRAM_CHAT_ID
    """
    token = _env("TELEGRAM_BOT_TOKEN")
    chat_id = _env("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return

    dag_id = context.get("dag", getattr(context.get("dag_run"), "dag_id", None))
    if hasattr(dag_id, "dag_id"):
        dag_id = dag_id.dag_id

    task_instance = context.get("task_instance")
    task_id = getattr(task_instance, "task_id", None)
    run_id = getattr(context.get("dag_run"), "run_id", None)
    log_url = getattr(task_instance, "log_url", None)
    exception = context.get("exception")

    lines = [
        "Airflow task failed",
        f"DAG: {dag_id}",
        f"Task: {task_id}",
        f"Run: {run_id}",
    ]
    if exception is not None:
        lines.append(f"Error: {exception}")
    if log_url:
        lines.append(f"Logs: {log_url}")

    text = "\n".join([ln for ln in lines if ln and "None" not in str(ln)])

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests.post(
            url,
            json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
            timeout=10,
        )
    except Exception:
        # Avoid breaking Airflow callbacks if Telegram is unavailable
        return

