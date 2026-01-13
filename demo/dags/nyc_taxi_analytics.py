"""
NYC Taxi Trip Analytics DAG

This DAG simulates processing NYC Yellow Taxi trip data.
Uses public dataset patterns from NYC TLC.
"""

from datetime import datetime, timedelta
import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=2),
}


def extract_taxi_data(**context):
    """Simulate extracting taxi trip data."""
    import time
    
    execution_date = context["execution_date"]
    print(f"Extracting NYC taxi data for {execution_date}")
    
    # Simulate varying extraction times
    sleep_time = random.uniform(1, 5)
    time.sleep(sleep_time)
    
    # Simulate occasional failures (10% chance)
    if random.random() < 0.1:
        raise Exception("Failed to connect to data source")
    
    record_count = random.randint(50000, 200000)
    print(f"Extracted {record_count} taxi trip records")
    return record_count


def validate_taxi_data(**context):
    """Validate extracted taxi data."""
    import time
    
    ti = context["ti"]
    record_count = ti.xcom_pull(task_ids="extract_taxi_data")
    
    print(f"Validating {record_count} records...")
    time.sleep(random.uniform(0.5, 2))
    
    # Simulate validation failures (5% chance)
    if random.random() < 0.05:
        raise Exception("Data validation failed: Missing required columns")
    
    valid_records = int(record_count * random.uniform(0.95, 0.99))
    print(f"Validation complete: {valid_records} valid records")
    return valid_records


def transform_taxi_data(**context):
    """Transform and enrich taxi data."""
    import time
    
    ti = context["ti"]
    valid_records = ti.xcom_pull(task_ids="validate_taxi_data")
    
    print(f"Transforming {valid_records} records...")
    
    # Simulate longer processing time
    time.sleep(random.uniform(2, 8))
    
    transformations = [
        "Calculating trip duration",
        "Geocoding pickup/dropoff locations",
        "Computing fare breakdown",
        "Adding weather data",
        "Categorizing trip types",
    ]
    
    for t in transformations:
        print(f"  - {t}")
        time.sleep(0.5)
    
    return valid_records


def calculate_metrics(**context):
    """Calculate daily trip metrics."""
    import time
    
    ti = context["ti"]
    records = ti.xcom_pull(task_ids="transform_taxi_data")
    
    print(f"Calculating metrics for {records} records...")
    time.sleep(random.uniform(1, 3))
    
    metrics = {
        "total_trips": records,
        "avg_fare": round(random.uniform(15, 35), 2),
        "avg_distance": round(random.uniform(2, 8), 2),
        "avg_duration_mins": round(random.uniform(10, 30), 2),
        "peak_hour": random.randint(7, 19),
    }
    
    print(f"Metrics: {metrics}")
    return metrics


def load_to_warehouse(**context):
    """Load processed data to data warehouse."""
    import time
    
    ti = context["ti"]
    records = ti.xcom_pull(task_ids="transform_taxi_data")
    metrics = ti.xcom_pull(task_ids="calculate_metrics")
    
    print(f"Loading {records} records to warehouse...")
    time.sleep(random.uniform(1, 4))
    
    # Simulate occasional load failures (5% chance)
    if random.random() < 0.05:
        raise Exception("Warehouse connection timeout")
    
    print("Data successfully loaded to warehouse")
    print(f"Daily metrics stored: {metrics}")


with DAG(
    dag_id="nyc_taxi_analytics",
    default_args=default_args,
    description="Process NYC Yellow Taxi trip data",
    schedule_interval="0 6 * * *",  # Daily at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["analytics", "taxi", "nyc", "daily"],
) as dag:

    start = EmptyOperator(task_id="start")
    
    extract = PythonOperator(
        task_id="extract_taxi_data",
        python_callable=extract_taxi_data,
    )
    
    validate = PythonOperator(
        task_id="validate_taxi_data",
        python_callable=validate_taxi_data,
    )
    
    transform = PythonOperator(
        task_id="transform_taxi_data",
        python_callable=transform_taxi_data,
    )
    
    metrics = PythonOperator(
        task_id="calculate_metrics",
        python_callable=calculate_metrics,
    )
    
    load = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse,
    )
    
    end = EmptyOperator(task_id="end")

    start >> extract >> validate >> transform >> [metrics, load] >> end
