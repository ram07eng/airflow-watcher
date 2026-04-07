"""
Social Media Analytics DAG

This DAG simulates collecting social media metrics.
Designed to occasionally fail for testing monitoring.
"""

from datetime import datetime, timedelta
import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "marketing-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "sla": timedelta(minutes=45),
}


PLATFORMS = ["twitter", "instagram", "facebook", "linkedin", "tiktok"]


def fetch_twitter_data(**context):
    """Fetch Twitter/X metrics."""
    import time
    
    print("Fetching Twitter data...")
    time.sleep(random.uniform(1, 3))
    
    # Higher failure rate for testing (25%)
    if random.random() < 0.25:
        raise Exception("Twitter API authentication failed")
    
    return {"followers": random.randint(10000, 50000), "engagement": random.uniform(2, 8)}


def fetch_instagram_data(**context):
    """Fetch Instagram metrics."""
    import time
    
    print("Fetching Instagram data...")
    time.sleep(random.uniform(1, 3))
    
    # 15% failure rate
    if random.random() < 0.15:
        raise Exception("Instagram rate limit exceeded")
    
    return {"followers": random.randint(20000, 100000), "engagement": random.uniform(3, 10)}


def fetch_facebook_data(**context):
    """Fetch Facebook metrics."""
    import time
    
    print("Fetching Facebook data...")
    time.sleep(random.uniform(1, 3))
    
    return {"followers": random.randint(5000, 30000), "engagement": random.uniform(1, 5)}


def fetch_linkedin_data(**context):
    """Fetch LinkedIn metrics."""
    import time
    
    print("Fetching LinkedIn data...")
    time.sleep(random.uniform(1, 3))
    
    # 10% failure rate
    if random.random() < 0.10:
        raise Exception("LinkedIn API timeout")
    
    return {"followers": random.randint(8000, 40000), "engagement": random.uniform(2, 6)}


def fetch_tiktok_data(**context):
    """Fetch TikTok metrics."""
    import time
    
    print("Fetching TikTok data...")
    time.sleep(random.uniform(2, 5))  # Slower API
    
    # 20% failure rate
    if random.random() < 0.20:
        raise Exception("TikTok API connection refused")
    
    return {"followers": random.randint(50000, 200000), "engagement": random.uniform(5, 15)}


def aggregate_metrics(**context):
    """Aggregate all social media metrics."""
    import time
    
    ti = context["ti"]
    
    platforms = {
        "twitter": ti.xcom_pull(task_ids="fetch_twitter"),
        "instagram": ti.xcom_pull(task_ids="fetch_instagram"),
        "facebook": ti.xcom_pull(task_ids="fetch_facebook"),
        "linkedin": ti.xcom_pull(task_ids="fetch_linkedin"),
        "tiktok": ti.xcom_pull(task_ids="fetch_tiktok"),
    }
    
    print("Aggregating social media metrics...")
    time.sleep(random.uniform(0.5, 1.5))
    
    total_followers = 0
    avg_engagement = 0
    count = 0
    
    for platform, data in platforms.items():
        if data:
            total_followers += data["followers"]
            avg_engagement += data["engagement"]
            count += 1
    
    if count > 0:
        avg_engagement /= count
    
    summary = {
        "total_followers": total_followers,
        "avg_engagement": round(avg_engagement, 2),
        "platforms_collected": count,
        "platforms_failed": len(platforms) - count,
    }
    
    print(f"Summary: {summary}")
    return summary


def generate_report(**context):
    """Generate social media report."""
    import time
    
    ti = context["ti"]
    summary = ti.xcom_pull(task_ids="aggregate_metrics")
    
    print("Generating social media report...")
    time.sleep(random.uniform(1, 2))
    
    print(f"📱 Social Media Report")
    print(f"   Total Followers: {summary['total_followers']:,}")
    print(f"   Avg Engagement: {summary['avg_engagement']}%")
    print(f"   Platforms OK: {summary['platforms_collected']}/5")


with DAG(
    dag_id="social_media_analytics",
    default_args=default_args,
    description="Collect social media metrics across platforms",
    schedule_interval="0 */2 * * *",  # Every 2 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["social", "marketing", "analytics"],
) as dag:

    start = EmptyOperator(task_id="start")
    
    twitter = PythonOperator(
        task_id="fetch_twitter",
        python_callable=fetch_twitter_data,
    )
    
    instagram = PythonOperator(
        task_id="fetch_instagram",
        python_callable=fetch_instagram_data,
    )
    
    facebook = PythonOperator(
        task_id="fetch_facebook",
        python_callable=fetch_facebook_data,
    )
    
    linkedin = PythonOperator(
        task_id="fetch_linkedin",
        python_callable=fetch_linkedin_data,
    )
    
    tiktok = PythonOperator(
        task_id="fetch_tiktok",
        python_callable=fetch_tiktok_data,
    )
    
    aggregate = PythonOperator(
        task_id="aggregate_metrics",
        python_callable=aggregate_metrics,
        trigger_rule="all_done",  # Run even if some platforms fail
    )
    
    report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )
    
    end = EmptyOperator(task_id="end")

    start >> [twitter, instagram, facebook, linkedin, tiktok] >> aggregate >> report >> end
