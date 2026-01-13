"""
Stock Market Data Collector DAG

This DAG simulates collecting stock market data.
Patterns based on public financial data APIs.
"""

from datetime import datetime, timedelta
import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_delta import TimeDeltaSensor


default_args = {
    "owner": "finance-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 5,  # More retries for financial data
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(minutes=15),  # Tight SLA for market data
}

SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "META", "TSLA", "NVDA", "JPM", "V", "JNJ"]


def check_market_hours(**context):
    """Check if market is open."""
    import time
    
    execution_date = context["execution_date"]
    hour = execution_date.hour
    weekday = execution_date.weekday()
    
    print(f"Checking market status at {execution_date}")
    time.sleep(0.5)
    
    # Market hours: Mon-Fri, 9:30 AM - 4:00 PM ET
    is_open = weekday < 5 and 9 <= hour < 16
    status = "OPEN" if is_open else "CLOSED"
    
    print(f"Market status: {status}")
    return is_open


def fetch_stock_prices(**context):
    """Fetch current stock prices."""
    import time
    
    print(f"Fetching prices for {len(SYMBOLS)} symbols...")
    time.sleep(random.uniform(1, 3))
    
    # Simulate API rate limiting (20% chance for testing)
    if random.random() < 0.20:
        raise Exception("API rate limit exceeded - retrying")
    
    prices = {}
    for symbol in SYMBOLS:
        prices[symbol] = {
            "price": round(random.uniform(50, 500), 2),
            "change": round(random.uniform(-5, 5), 2),
            "volume": random.randint(1000000, 50000000),
        }
    
    print(f"Fetched prices for {len(prices)} symbols")
    return prices


def fetch_market_indices(**context):
    """Fetch major market indices."""
    import time
    
    print("Fetching market indices...")
    time.sleep(random.uniform(0.5, 2))
    
    indices = {
        "SPY": round(random.uniform(400, 500), 2),
        "DIA": round(random.uniform(300, 400), 2),
        "QQQ": round(random.uniform(350, 450), 2),
        "VIX": round(random.uniform(12, 35), 2),
    }
    
    print(f"Indices: {indices}")
    return indices


def calculate_portfolio_value(**context):
    """Calculate portfolio value."""
    import time
    
    ti = context["ti"]
    prices = ti.xcom_pull(task_ids="fetch_stock_prices")
    
    print("Calculating portfolio value...")
    time.sleep(random.uniform(0.5, 1.5))
    
    # Simulate portfolio holdings
    holdings = {symbol: random.randint(10, 100) for symbol in SYMBOLS[:5]}
    
    total_value = sum(
        prices[symbol]["price"] * shares 
        for symbol, shares in holdings.items()
    )
    
    portfolio = {
        "holdings": holdings,
        "total_value": round(total_value, 2),
        "daily_change": round(random.uniform(-2, 2), 2),
    }
    
    print(f"Portfolio value: ${portfolio['total_value']}")
    return portfolio


def detect_anomalies(**context):
    """Detect price anomalies."""
    import time
    
    ti = context["ti"]
    prices = ti.xcom_pull(task_ids="fetch_stock_prices")
    
    print("Running anomaly detection...")
    time.sleep(random.uniform(1, 2))
    
    anomalies = []
    for symbol, data in prices.items():
        if abs(data["change"]) > 3:
            anomalies.append({
                "symbol": symbol,
                "change": data["change"],
                "alert_type": "HIGH_VOLATILITY",
            })
    
    if anomalies:
        print(f"Detected {len(anomalies)} anomalies: {anomalies}")
    else:
        print("No anomalies detected")
    
    return anomalies


def store_market_data(**context):
    """Store market data to database."""
    import time
    
    ti = context["ti"]
    prices = ti.xcom_pull(task_ids="fetch_stock_prices")
    indices = ti.xcom_pull(task_ids="fetch_market_indices")
    
    print("Storing market data...")
    time.sleep(random.uniform(0.5, 2))
    
    # Simulate storage failures (5% chance)
    if random.random() < 0.05:
        raise Exception("Database write failed - disk full")
    
    print(f"Stored {len(prices)} stock prices and {len(indices)} indices")


def generate_alerts(**context):
    """Generate trading alerts."""
    import time
    
    ti = context["ti"]
    anomalies = ti.xcom_pull(task_ids="detect_anomalies")
    portfolio = ti.xcom_pull(task_ids="calculate_portfolio_value")
    
    print("Generating alerts...")
    time.sleep(random.uniform(0.5, 1))
    
    alerts = []
    
    if anomalies:
        for a in anomalies:
            alerts.append(f"VOLATILITY ALERT: {a['symbol']} changed {a['change']}%")
    
    if portfolio["daily_change"] < -1:
        alerts.append(f"PORTFOLIO ALERT: Down {portfolio['daily_change']}% today")
    
    for alert in alerts:
        print(f"  📊 {alert}")
    
    return alerts


with DAG(
    dag_id="stock_market_collector",
    default_args=default_args,
    description="Collect stock market data during trading hours",
    schedule_interval="*/5 9-16 * * 1-5",  # Every 5 min during market hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["finance", "stocks", "streaming", "high-frequency"],
) as dag:

    start = EmptyOperator(task_id="start")
    
    check_market = PythonOperator(
        task_id="check_market_hours",
        python_callable=check_market_hours,
    )
    
    fetch_prices = PythonOperator(
        task_id="fetch_stock_prices",
        python_callable=fetch_stock_prices,
    )
    
    fetch_indices = PythonOperator(
        task_id="fetch_market_indices",
        python_callable=fetch_market_indices,
    )
    
    portfolio = PythonOperator(
        task_id="calculate_portfolio_value",
        python_callable=calculate_portfolio_value,
    )
    
    anomalies = PythonOperator(
        task_id="detect_anomalies",
        python_callable=detect_anomalies,
    )
    
    store = PythonOperator(
        task_id="store_market_data",
        python_callable=store_market_data,
    )
    
    alerts = PythonOperator(
        task_id="generate_alerts",
        python_callable=generate_alerts,
    )
    
    end = EmptyOperator(task_id="end")

    start >> check_market >> [fetch_prices, fetch_indices]
    fetch_prices >> [portfolio, anomalies, store]
    fetch_indices >> store
    [portfolio, anomalies] >> alerts >> end
    store >> end
