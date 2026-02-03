from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["lib-testmail@lists.berkeley.edu"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}


with DAG(
    "sample_dag",
    default_args=default_args,
    description="A sample Dag",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 2, 2)
) as dag:
    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")

    start_task >> end_task # pyright: ignore[reportUnusedExpression]
