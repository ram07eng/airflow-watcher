"""
E-Commerce Sales ETL DAG

This DAG simulates processing e-commerce sales data.
Patterns based on public retail datasets.
"""

from datetime import datetime, timedelta
import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup


default_args = {
    "owner": "ecommerce-team",
    "depends_on_past": True,  # Dependencies on previous runs
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "sla": timedelta(hours=4),
}


def extract_orders(**context):
    """Extract orders from source systems."""
    import time
    
    print("Extracting orders from database...")
    time.sleep(random.uniform(2, 5))
    
    order_count = random.randint(1000, 5000)
    print(f"Extracted {order_count} orders")
    
    # Simulate occasional extraction failures (8% chance)
    if random.random() < 0.08:
        raise Exception("Database connection lost during extraction")
    
    return order_count


def extract_products(**context):
    """Extract product catalog."""
    import time
    
    print("Extracting product catalog...")
    time.sleep(random.uniform(1, 3))
    
    product_count = random.randint(500, 2000)
    print(f"Extracted {product_count} products")
    return product_count


def extract_customers(**context):
    """Extract customer data."""
    import time
    
    print("Extracting customer data...")
    time.sleep(random.uniform(1, 4))
    
    # Simulate PII handling delay
    customer_count = random.randint(800, 3000)
    print(f"Extracted {customer_count} customers (PII masked)")
    return customer_count


def transform_orders(**context):
    """Transform and enrich order data."""
    import time
    
    ti = context["ti"]
    orders = ti.xcom_pull(task_ids="extract.extract_orders")
    
    print(f"Transforming {orders} orders...")
    time.sleep(random.uniform(3, 8))
    
    # Simulate transformation failure (5% chance)
    if random.random() < 0.05:
        raise Exception("Schema mismatch in order transformation")
    
    print("Order transformations complete")
    return orders


def transform_products(**context):
    """Transform product data."""
    import time
    
    ti = context["ti"]
    products = ti.xcom_pull(task_ids="extract.extract_products")
    
    print(f"Transforming {products} products...")
    time.sleep(random.uniform(1, 3))
    
    print("Product transformations complete")
    return products


def transform_customers(**context):
    """Transform customer data with privacy compliance."""
    import time
    
    ti = context["ti"]
    customers = ti.xcom_pull(task_ids="extract.extract_customers")
    
    print(f"Transforming {customers} customers (GDPR compliant)...")
    time.sleep(random.uniform(2, 5))
    
    print("Customer transformations complete")
    return customers


def join_datasets(**context):
    """Join all transformed datasets."""
    import time
    
    ti = context["ti"]
    orders = ti.xcom_pull(task_ids="transform.transform_orders")
    products = ti.xcom_pull(task_ids="transform.transform_products")
    customers = ti.xcom_pull(task_ids="transform.transform_customers")
    
    print(f"Joining datasets: {orders} orders, {products} products, {customers} customers")
    time.sleep(random.uniform(3, 7))
    
    # Simulate join failures (3% chance)
    if random.random() < 0.03:
        raise Exception("Memory exceeded during large join operation")
    
    enriched_count = orders
    print(f"Created {enriched_count} enriched sales records")
    return enriched_count


def calculate_revenue(**context):
    """Calculate revenue metrics."""
    import time
    
    ti = context["ti"]
    records = ti.xcom_pull(task_ids="join_datasets")
    
    print(f"Calculating revenue from {records} records...")
    time.sleep(random.uniform(1, 3))
    
    revenue = {
        "total_revenue": round(random.uniform(50000, 200000), 2),
        "avg_order_value": round(random.uniform(50, 150), 2),
        "top_category": random.choice(["Electronics", "Clothing", "Home", "Sports"]),
    }
    
    print(f"Revenue metrics: {revenue}")
    return revenue


def calculate_customer_metrics(**context):
    """Calculate customer behavior metrics."""
    import time
    
    ti = context["ti"]
    records = ti.xcom_pull(task_ids="join_datasets")
    
    print(f"Calculating customer metrics from {records} records...")
    time.sleep(random.uniform(1, 3))
    
    metrics = {
        "new_customers": random.randint(50, 200),
        "returning_customers": random.randint(200, 800),
        "churn_rate": round(random.uniform(0.02, 0.08), 3),
    }
    
    print(f"Customer metrics: {metrics}")
    return metrics


def load_to_warehouse(**context):
    """Load all data to data warehouse."""
    import time
    
    ti = context["ti"]
    records = ti.xcom_pull(task_ids="join_datasets")
    revenue = ti.xcom_pull(task_ids="metrics.calculate_revenue")
    customer_metrics = ti.xcom_pull(task_ids="metrics.calculate_customer_metrics")
    
    print(f"Loading {records} records to warehouse...")
    time.sleep(random.uniform(2, 5))
    
    print("Warehouse load complete")
    print(f"Revenue: ${revenue['total_revenue']}")
    print(f"New customers: {customer_metrics['new_customers']}")


def send_daily_report(**context):
    """Send daily sales report."""
    import time
    
    print("Generating and sending daily sales report...")
    time.sleep(random.uniform(1, 2))
    
    print("Daily report sent to stakeholders")


with DAG(
    dag_id="ecommerce_sales_etl",
    default_args=default_args,
    description="Daily e-commerce sales data ETL pipeline",
    schedule_interval="0 2 * * *",  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "ecommerce", "sales", "daily"],
) as dag:

    start = EmptyOperator(task_id="start")
    
    # Extract task group
    with TaskGroup("extract") as extract_group:
        orders = PythonOperator(
            task_id="extract_orders",
            python_callable=extract_orders,
        )
        products = PythonOperator(
            task_id="extract_products",
            python_callable=extract_products,
        )
        customers = PythonOperator(
            task_id="extract_customers",
            python_callable=extract_customers,
        )
    
    # Transform task group
    with TaskGroup("transform") as transform_group:
        t_orders = PythonOperator(
            task_id="transform_orders",
            python_callable=transform_orders,
        )
        t_products = PythonOperator(
            task_id="transform_products",
            python_callable=transform_products,
        )
        t_customers = PythonOperator(
            task_id="transform_customers",
            python_callable=transform_customers,
        )
    
    join = PythonOperator(
        task_id="join_datasets",
        python_callable=join_datasets,
    )
    
    # Metrics task group
    with TaskGroup("metrics") as metrics_group:
        revenue = PythonOperator(
            task_id="calculate_revenue",
            python_callable=calculate_revenue,
        )
        customer_metrics = PythonOperator(
            task_id="calculate_customer_metrics",
            python_callable=calculate_customer_metrics,
        )
    
    load = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse,
    )
    
    report = PythonOperator(
        task_id="send_daily_report",
        python_callable=send_daily_report,
    )
    
    end = EmptyOperator(task_id="end")

    start >> extract_group >> transform_group >> join >> metrics_group >> load >> report >> end
