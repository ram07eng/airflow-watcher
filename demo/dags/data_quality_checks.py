"""
Data Quality Checks DAG

This DAG runs data quality checks across multiple datasets.
Demonstrates SLA tracking and alerting patterns.
"""

from datetime import datetime, timedelta
import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "data-quality-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=1),  # Strict SLA for quality checks
}


DATASETS = [
    {"name": "orders", "table": "raw.orders", "critical": True},
    {"name": "customers", "table": "raw.customers", "critical": True},
    {"name": "products", "table": "raw.products", "critical": False},
    {"name": "inventory", "table": "raw.inventory", "critical": True},
    {"name": "shipments", "table": "raw.shipments", "critical": False},
]


def check_row_counts(**context):
    """Check row counts meet minimum thresholds."""
    import time
    
    print("Running row count checks...")
    results = {}
    
    for dataset in DATASETS:
        time.sleep(random.uniform(0.5, 1.5))
        
        row_count = random.randint(1000, 100000)
        threshold = 500
        passed = row_count >= threshold
        
        results[dataset["name"]] = {
            "row_count": row_count,
            "threshold": threshold,
            "passed": passed,
        }
        
        status = "✅" if passed else "❌"
        print(f"  {status} {dataset['name']}: {row_count} rows (min: {threshold})")
    
    # Simulate occasional failures (10% chance)
    if random.random() < 0.1:
        raise Exception("Row count check failed: orders table below threshold")
    
    return results


def check_null_values(**context):
    """Check for unexpected null values in critical columns."""
    import time
    
    print("Running null value checks...")
    results = {}
    
    for dataset in DATASETS:
        time.sleep(random.uniform(0.3, 1))
        
        null_percentage = random.uniform(0, 5)
        threshold = 2.0
        passed = null_percentage <= threshold
        
        results[dataset["name"]] = {
            "null_percentage": round(null_percentage, 2),
            "threshold": threshold,
            "passed": passed,
        }
        
        status = "✅" if passed else "❌"
        print(f"  {status} {dataset['name']}: {null_percentage:.2f}% nulls (max: {threshold}%)")
    
    return results


def check_freshness(**context):
    """Check data freshness meets requirements."""
    import time
    
    print("Running freshness checks...")
    results = {}
    
    for dataset in DATASETS:
        time.sleep(random.uniform(0.2, 0.8))
        
        hours_old = random.uniform(0, 48)
        threshold_hours = 24
        passed = hours_old <= threshold_hours
        
        results[dataset["name"]] = {
            "hours_old": round(hours_old, 1),
            "threshold_hours": threshold_hours,
            "passed": passed,
        }
        
        status = "✅" if passed else "❌"
        print(f"  {status} {dataset['name']}: {hours_old:.1f} hours old (max: {threshold_hours}h)")
    
    # Simulate stale data (8% chance)
    if random.random() < 0.08:
        raise Exception("Freshness check failed: inventory data is 36 hours old")
    
    return results


def check_schema_drift(**context):
    """Check for unexpected schema changes."""
    import time
    
    print("Running schema drift checks...")
    time.sleep(random.uniform(1, 3))
    
    # Simulate schema changes (5% chance)
    if random.random() < 0.05:
        raise Exception("Schema drift detected: new column 'extra_field' in orders table")
    
    print("  ✅ No schema drift detected")
    return {"schema_drift": False}


def check_duplicates(**context):
    """Check for duplicate records."""
    import time
    
    print("Running duplicate checks...")
    results = {}
    
    for dataset in DATASETS:
        time.sleep(random.uniform(0.3, 1))
        
        duplicate_count = random.randint(0, 50)
        passed = duplicate_count < 10
        
        results[dataset["name"]] = {
            "duplicate_count": duplicate_count,
            "passed": passed,
        }
        
        status = "✅" if passed else "❌"
        print(f"  {status} {dataset['name']}: {duplicate_count} duplicates")
    
    return results


def aggregate_results(**context):
    """Aggregate all quality check results."""
    import time
    
    ti = context["ti"]
    
    row_counts = ti.xcom_pull(task_ids="check_row_counts")
    nulls = ti.xcom_pull(task_ids="check_null_values")
    freshness = ti.xcom_pull(task_ids="check_freshness")
    schema = ti.xcom_pull(task_ids="check_schema_drift")
    duplicates = ti.xcom_pull(task_ids="check_duplicates")
    
    print("Aggregating quality check results...")
    time.sleep(random.uniform(0.5, 1))
    
    total_checks = 0
    passed_checks = 0
    failed_datasets = []
    
    for dataset in DATASETS:
        name = dataset["name"]
        checks_passed = all([
            row_counts.get(name, {}).get("passed", True),
            nulls.get(name, {}).get("passed", True),
            freshness.get(name, {}).get("passed", True),
            duplicates.get(name, {}).get("passed", True),
        ])
        
        total_checks += 4
        passed_checks += sum([
            row_counts.get(name, {}).get("passed", True),
            nulls.get(name, {}).get("passed", True),
            freshness.get(name, {}).get("passed", True),
            duplicates.get(name, {}).get("passed", True),
        ])
        
        if not checks_passed:
            failed_datasets.append(name)
    
    summary = {
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "pass_rate": round(passed_checks / total_checks * 100, 1),
        "failed_datasets": failed_datasets,
    }
    
    print(f"\n📊 Quality Summary:")
    print(f"   Pass Rate: {summary['pass_rate']}%")
    print(f"   Passed: {passed_checks}/{total_checks}")
    
    if failed_datasets:
        print(f"   Failed Datasets: {failed_datasets}")
    
    return summary


def send_quality_report(**context):
    """Send quality report to stakeholders."""
    import time
    
    ti = context["ti"]
    summary = ti.xcom_pull(task_ids="aggregate_results")
    
    print("Sending quality report...")
    time.sleep(random.uniform(0.5, 1.5))
    
    if summary["pass_rate"] < 90:
        print("⚠️  Quality issues detected - escalating to data team")
    else:
        print("✅ Quality report sent - all checks passed")


with DAG(
    dag_id="data_quality_checks",
    default_args=default_args,
    description="Run data quality checks across all datasets",
    schedule_interval="0 */4 * * *",  # Every 4 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data-quality", "monitoring", "critical"],
) as dag:

    start = EmptyOperator(task_id="start")
    
    row_counts = PythonOperator(
        task_id="check_row_counts",
        python_callable=check_row_counts,
    )
    
    nulls = PythonOperator(
        task_id="check_null_values",
        python_callable=check_null_values,
    )
    
    freshness = PythonOperator(
        task_id="check_freshness",
        python_callable=check_freshness,
    )
    
    schema = PythonOperator(
        task_id="check_schema_drift",
        python_callable=check_schema_drift,
    )
    
    duplicates = PythonOperator(
        task_id="check_duplicates",
        python_callable=check_duplicates,
    )
    
    aggregate = PythonOperator(
        task_id="aggregate_results",
        python_callable=aggregate_results,
    )
    
    report = PythonOperator(
        task_id="send_quality_report",
        python_callable=send_quality_report,
    )
    
    end = EmptyOperator(task_id="end")

    start >> [row_counts, nulls, freshness, schema, duplicates] >> aggregate >> report >> end
