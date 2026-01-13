"""
Weather Data Pipeline DAG

This DAG simulates collecting and processing weather data.
Uses patterns from NOAA/OpenWeather public APIs.
"""

from datetime import datetime, timedelta
import random

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "sla": timedelta(minutes=30),
}

CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin"
]


def fetch_weather_api(**context):
    """Fetch current weather data from API."""
    import time
    
    execution_date = context["execution_date"]
    print(f"Fetching weather data for {execution_date}")
    
    time.sleep(random.uniform(1, 3))
    
    # Simulate API failures (15% chance for testing)
    if random.random() < 0.15:
        raise Exception("Weather API rate limit exceeded")
    
    weather_data = []
    for city in CITIES:
        weather_data.append({
            "city": city,
            "temp_f": round(random.uniform(20, 95), 1),
            "humidity": random.randint(30, 90),
            "wind_mph": round(random.uniform(0, 30), 1),
            "conditions": random.choice(["Clear", "Cloudy", "Rain", "Snow", "Fog"]),
        })
    
    print(f"Fetched weather for {len(weather_data)} cities")
    return weather_data


def check_severe_weather(**context):
    """Check for severe weather conditions and branch."""
    ti = context["ti"]
    weather_data = ti.xcom_pull(task_ids="fetch_weather_api")
    
    severe_conditions = []
    for city_weather in weather_data:
        if city_weather["wind_mph"] > 25:
            severe_conditions.append(f"{city_weather['city']}: High winds")
        if city_weather["temp_f"] > 100 or city_weather["temp_f"] < 10:
            severe_conditions.append(f"{city_weather['city']}: Extreme temp")
    
    if severe_conditions:
        print(f"Severe weather detected: {severe_conditions}")
        return "send_severe_alert"
    else:
        print("No severe weather conditions")
        return "store_weather_data"


def send_severe_alert(**context):
    """Send severe weather alerts."""
    import time
    
    ti = context["ti"]
    weather_data = ti.xcom_pull(task_ids="fetch_weather_api")
    
    print("Sending severe weather alerts...")
    time.sleep(random.uniform(0.5, 1.5))
    
    for city_weather in weather_data:
        if city_weather["wind_mph"] > 25 or city_weather["temp_f"] > 100 or city_weather["temp_f"] < 10:
            print(f"  ALERT: {city_weather['city']} - {city_weather['conditions']}, "
                  f"Temp: {city_weather['temp_f']}F, Wind: {city_weather['wind_mph']}mph")


def store_weather_data(**context):
    """Store weather data to database."""
    import time
    
    ti = context["ti"]
    weather_data = ti.xcom_pull(task_ids="fetch_weather_api")
    
    print(f"Storing {len(weather_data)} weather records...")
    time.sleep(random.uniform(0.5, 2))
    
    print("Weather data stored successfully")


def calculate_daily_summary(**context):
    """Calculate daily weather summary."""
    import time
    
    ti = context["ti"]
    weather_data = ti.xcom_pull(task_ids="fetch_weather_api")
    
    temps = [w["temp_f"] for w in weather_data]
    
    summary = {
        "avg_temp": round(sum(temps) / len(temps), 1),
        "max_temp": max(temps),
        "min_temp": min(temps),
        "cities_with_rain": sum(1 for w in weather_data if w["conditions"] == "Rain"),
    }
    
    time.sleep(random.uniform(0.5, 1))
    print(f"Daily summary: {summary}")
    return summary


with DAG(
    dag_id="weather_data_pipeline",
    default_args=default_args,
    description="Collect and process weather data from public APIs",
    schedule_interval="*/30 * * * *",  # Every 30 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weather", "api", "streaming"],
) as dag:

    start = EmptyOperator(task_id="start")
    
    fetch = PythonOperator(
        task_id="fetch_weather_api",
        python_callable=fetch_weather_api,
    )
    
    check_severe = BranchPythonOperator(
        task_id="check_severe_weather",
        python_callable=check_severe_weather,
    )
    
    alert = PythonOperator(
        task_id="send_severe_alert",
        python_callable=send_severe_alert,
    )
    
    store = PythonOperator(
        task_id="store_weather_data",
        python_callable=store_weather_data,
        trigger_rule="none_failed_min_one_success",
    )
    
    summary = PythonOperator(
        task_id="calculate_daily_summary",
        python_callable=calculate_daily_summary,
        trigger_rule="none_failed_min_one_success",
    )
    
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    start >> fetch >> check_severe
    check_severe >> alert >> store
    check_severe >> store
    store >> summary >> end
