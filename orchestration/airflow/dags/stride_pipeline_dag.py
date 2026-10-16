"""
AIRFLOW DAG: the Stride pipeline, one task per step
ingestion >> silver >> gold >> export >> verify
Each task calls main.execute(<command>), the same function the command line uses, so there is one copy of the logic.
Started by hand ('make airflow-run'); watch it in the Airflow web page.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import main

STEPS = [
    ("ingestion", "ingest"),
    ("silver", "silver"),
    ("gold", "gold"),
    ("export", "export"),
    ("verify", "verify"),
]

with DAG(
    dag_id="stride_pipeline",
    description="Stride pipeline: ingestion, silver, gold, export, verify",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,  # DuckDB allows one writer at a time
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["stride"],
) as dag:
    tasks = [
        PythonOperator(task_id=task_id, python_callable=main.execute, op_args=[command])
        for task_id, command in STEPS
    ]
    for upstream, downstream in zip(tasks, tasks[1:], strict=False):
        upstream >> downstream
