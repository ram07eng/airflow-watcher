"""Example DAG with SLA for testing Airflow Watcher."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def task_that_might_fail(**context):
    """A task that randomly fails for testing."""
    import random
    if random.random() < 0.3:  # 30% chance of failure
        raise Exception("Random failure for testing!")
    print("Task completed successfully!")


def slow_task(**context):
    """A task that might exceed SLA."""
    import time
    import random
    sleep_time = random.randint(1, 60)  # 1-60 seconds
    print(f"Sleeping for {sleep_time} seconds...")
    time.sleep(sleep_time)
    print("Slow task completed!")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="example_watcher_test_dag",
    default_args=default_args,
    description="Example DAG for testing Airflow Watcher plugin",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "watcher-test"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Starting DAG run'",
    )

    might_fail = PythonOperator(
        task_id="might_fail",
        python_callable=task_that_might_fail,
        sla=timedelta(seconds=30),  # SLA of 30 seconds
    )

    slow = PythonOperator(
        task_id="slow_task",
        python_callable=slow_task,
        sla=timedelta(seconds=10),  # SLA of 10 seconds (likely to miss)
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'DAG run completed'",
    )

    start >> [might_fail, slow] >> end
