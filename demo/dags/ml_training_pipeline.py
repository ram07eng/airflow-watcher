"""
ML Model Training Pipeline DAG

This DAG simulates an ML training pipeline.
Has long-running tasks to test task health monitoring.
"""

from datetime import datetime, timedelta
import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "ml-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "sla": timedelta(hours=6),  # Long SLA for ML training
}


def prepare_training_data(**context):
    """Prepare training dataset."""
    import time
    
    print("Preparing training data...")
    print("  - Loading raw data from S3")
    time.sleep(random.uniform(2, 5))
    
    print("  - Cleaning and preprocessing")
    time.sleep(random.uniform(2, 5))
    
    print("  - Feature engineering")
    time.sleep(random.uniform(3, 7))
    
    # Simulate data prep failures (8%)
    if random.random() < 0.08:
        raise Exception("Memory error: Dataset too large for available RAM")
    
    records = random.randint(100000, 500000)
    print(f"Prepared {records:,} training samples")
    return records


def split_data(**context):
    """Split data into train/validation/test sets."""
    import time
    
    ti = context["ti"]
    records = ti.xcom_pull(task_ids="prepare_training_data")
    
    print(f"Splitting {records:,} records...")
    time.sleep(random.uniform(1, 3))
    
    splits = {
        "train": int(records * 0.7),
        "validation": int(records * 0.15),
        "test": int(records * 0.15),
    }
    
    print(f"Split: {splits}")
    return splits


def train_model(**context):
    """Train ML model - long running task."""
    import time
    
    ti = context["ti"]
    splits = ti.xcom_pull(task_ids="split_data")
    
    print(f"Training model on {splits['train']:,} samples...")
    
    epochs = 10
    for epoch in range(1, epochs + 1):
        # Simulate epoch training time (5-15 seconds each for demo)
        time.sleep(random.uniform(5, 15))
        
        loss = round(1.0 / (epoch + random.uniform(0.5, 1.5)), 4)
        accuracy = round(min(0.5 + epoch * 0.05 + random.uniform(-0.02, 0.02), 0.99), 4)
        
        print(f"  Epoch {epoch}/{epochs} - Loss: {loss}, Accuracy: {accuracy}")
    
    # Simulate training failures (5%)
    if random.random() < 0.05:
        raise Exception("CUDA out of memory: GPU memory exceeded")
    
    metrics = {
        "final_loss": loss,
        "final_accuracy": accuracy,
        "epochs": epochs,
    }
    
    print(f"Training complete: {metrics}")
    return metrics


def evaluate_model(**context):
    """Evaluate model on test set."""
    import time
    
    ti = context["ti"]
    splits = ti.xcom_pull(task_ids="split_data")
    train_metrics = ti.xcom_pull(task_ids="train_model")
    
    print(f"Evaluating model on {splits['test']:,} test samples...")
    time.sleep(random.uniform(3, 8))
    
    test_accuracy = train_metrics["final_accuracy"] - random.uniform(0.01, 0.05)
    
    eval_metrics = {
        "test_accuracy": round(test_accuracy, 4),
        "precision": round(random.uniform(0.85, 0.95), 4),
        "recall": round(random.uniform(0.80, 0.92), 4),
        "f1_score": round(random.uniform(0.82, 0.93), 4),
    }
    
    print(f"Evaluation metrics: {eval_metrics}")
    return eval_metrics


def register_model(**context):
    """Register model in model registry."""
    import time
    
    ti = context["ti"]
    eval_metrics = ti.xcom_pull(task_ids="evaluate_model")
    
    print("Registering model in registry...")
    time.sleep(random.uniform(1, 3))
    
    # Check if model meets quality threshold
    if eval_metrics["test_accuracy"] < 0.80:
        raise Exception(f"Model quality too low: {eval_metrics['test_accuracy']} < 0.80 threshold")
    
    model_version = f"v{random.randint(1, 100)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
    print(f"Model registered: {model_version}")
    
    return {"version": model_version, "metrics": eval_metrics}


def deploy_model(**context):
    """Deploy model to production."""
    import time
    
    ti = context["ti"]
    registration = ti.xcom_pull(task_ids="register_model")
    
    print(f"Deploying model {registration['version']} to production...")
    time.sleep(random.uniform(2, 5))
    
    # Simulate deployment failures (10%)
    if random.random() < 0.10:
        raise Exception("Deployment failed: Kubernetes pod scheduling error")
    
    print(f"✅ Model {registration['version']} deployed successfully")
    return {"status": "deployed", "version": registration["version"]}


def notify_stakeholders(**context):
    """Notify stakeholders of training completion."""
    import time
    
    ti = context["ti"]
    deployment = ti.xcom_pull(task_ids="deploy_model")
    eval_metrics = ti.xcom_pull(task_ids="evaluate_model")
    
    print("Sending notifications...")
    time.sleep(random.uniform(0.5, 1.5))
    
    print(f"📧 Training Pipeline Complete")
    print(f"   Model Version: {deployment['version']}")
    print(f"   Test Accuracy: {eval_metrics['test_accuracy']}")
    print(f"   Status: {deployment['status']}")


with DAG(
    dag_id="ml_training_pipeline",
    default_args=default_args,
    description="ML model training pipeline with long-running tasks",
    schedule_interval="0 0 * * 0",  # Weekly on Sunday
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ml", "training", "weekly", "long-running"],
) as dag:

    start = EmptyOperator(task_id="start")
    
    prepare = PythonOperator(
        task_id="prepare_training_data",
        python_callable=prepare_training_data,
    )
    
    split = PythonOperator(
        task_id="split_data",
        python_callable=split_data,
    )
    
    train = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
        execution_timeout=timedelta(hours=4),
    )
    
    evaluate = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluate_model,
    )
    
    register = PythonOperator(
        task_id="register_model",
        python_callable=register_model,
    )
    
    deploy = PythonOperator(
        task_id="deploy_model",
        python_callable=deploy_model,
    )
    
    notify = PythonOperator(
        task_id="notify_stakeholders",
        python_callable=notify_stakeholders,
    )
    
    end = EmptyOperator(task_id="end")

    start >> prepare >> split >> train >> evaluate >> register >> deploy >> notify >> end
