"""
Demo DAG to demonstrate SLA failures for Airflow Watcher plugin.
This DAG intentionally violates SLA to show monitoring capabilities.
"""
from datetime import datetime, timedelta
import time

from airflow import DAG
from airflow.operators.python import PythonOperator


def slow_task():
    """Task that takes longer than its SLA."""
    time.sleep(120)  # Sleep for 2 minutes


def sla_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Callback when SLA is missed."""
    print(f"SLA MISSED! Tasks: {task_list}")


default_args = {
    'owner': 'watcher-demo',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# DAG with very short SLA that will be missed
with DAG(
    dag_id='sla_failure_demo',
    default_args=default_args,
    description='Demo DAG that intentionally misses SLA',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['demo', 'sla', 'watcher'],
    sla_miss_callback=sla_callback,
) as dag:

    # Task with 10 second SLA that takes 2 minutes
    slow_processing = PythonOperator(
        task_id='slow_processing_task',
        python_callable=slow_task,
        sla=timedelta(seconds=10),  # Very short SLA - will be missed!
    )

    # Another task with SLA
    another_slow_task = PythonOperator(
        task_id='another_slow_task',
        python_callable=slow_task,
        sla=timedelta(seconds=5),  # Even shorter SLA
    )

    slow_processing >> another_slow_task
