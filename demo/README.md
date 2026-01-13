# Airflow Watcher Demo Environment

This is a local Airflow test environment with sample DAGs for testing the airflow-watcher plugin.

## Quick Start

```bash
# Navigate to demo folder
cd demo

# Start the environment
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the environment
docker-compose down
```

## Access

- **Airflow UI**: http://localhost:8080
- **Username**: admin
- **Password**: admin

## Sample DAGs

| DAG | Schedule | Description | Failure Rate |
|-----|----------|-------------|--------------|
| `nyc_taxi_analytics` | Daily 6 AM | NYC taxi trip data processing | 10-15% |
| `weather_data_pipeline` | Every 30 min | Weather data collection | 15% |
| `ecommerce_sales_etl` | Daily 2 AM | E-commerce ETL with task groups | 8-10% |
| `stock_market_collector` | Every 5 min (market hours) | Stock price collection | 20-25% |
| `data_quality_checks` | Every 4 hours | Data quality monitoring | 8-10% |
| `social_media_analytics` | Every 2 hours | Social media metrics | 15-25% |
| `ml_training_pipeline` | Weekly Sunday | ML model training (long tasks) | 10-15% |

## Testing the Watcher Plugin

Once Airflow is running, navigate to the **"Watcher"** menu in the top navigation:

1. **Airflow Dashboard** - Overview of all DAG health
2. **Airflow Health** - DAG health status (success/failed/delayed/stale)
3. **DAG Scheduling** - Queue and pool utilization
4. **DAG Failures** - Recent failure list with details
5. **SLA Tracker** - SLA misses and delay warnings
6. **Task Health** - Long-running and zombie tasks
7. **Dependencies** - Cross-DAG failure tracking

## API Endpoints

Test the REST API:

```bash
# Health check
curl http://localhost:8080/api/watcher/health

# Recent failures
curl http://localhost:8080/api/watcher/failures

# SLA misses
curl http://localhost:8080/api/watcher/sla/misses

# Task health
curl http://localhost:8080/api/watcher/tasks/long-running

# Scheduling status
curl http://localhost:8080/api/watcher/scheduling/lag
```

## Triggering Test Failures

The DAGs are designed with random failure rates to simulate real-world scenarios. You can also manually trigger DAGs:

```bash
# Trigger a DAG
docker-compose exec airflow-webserver airflow dags trigger social_media_analytics

# Trigger multiple runs
for i in {1..5}; do
  docker-compose exec airflow-webserver airflow dags trigger weather_data_pipeline
  sleep 5
done
```

## Folder Structure

```
airflow-watcher-demo/
├── docker-compose.yml     # Airflow setup
├── Makefile              # Convenience commands
├── dags/                 # Sample DAG files
│   ├── nyc_taxi_analytics.py
│   ├── weather_data_pipeline.py
│   ├── ecommerce_sales_etl.py
│   ├── stock_market_collector.py
│   ├── data_quality_checks.py
│   ├── social_media_analytics.py
│   └── ml_training_pipeline.py
├── logs/                 # Airflow logs (gitignored)
└── data/                 # Data files (gitignored)
```

## Troubleshooting

### Plugin not showing up
```bash
# Restart webserver to reload plugins
make restart
```

### Database issues
```bash
# Reset the database
make clean
make start
```

### Check plugin is loaded
```bash
docker-compose exec airflow-webserver airflow plugins
```
