.PHONY: start stop restart logs clean shell trigger-all

# Default target
help:
	@echo "Airflow Watcher Demo - Available commands:"
	@echo ""
	@echo "  make start       - Start Airflow environment"
	@echo "  make stop        - Stop Airflow environment"
	@echo "  make restart     - Restart Airflow environment"
	@echo "  make logs        - View Airflow logs"
	@echo "  make shell       - Open shell in webserver container"
	@echo "  make trigger-all - Trigger all sample DAGs"
	@echo "  make clean       - Remove all containers and volumes"
	@echo ""

# Set AIRFLOW_UID for Linux
export AIRFLOW_UID ?= $(shell id -u)

# Start the environment
start:
	@echo "Starting Airflow environment..."
	@mkdir -p logs data
	docker-compose up -d
	@echo ""
	@echo "Airflow is starting up..."
	@echo "UI will be available at: http://localhost:8080"
	@echo "Username: admin"
	@echo "Password: admin"
	@echo ""
	@echo "Run 'make logs' to view startup logs"

# Stop the environment
stop:
	@echo "Stopping Airflow environment..."
	docker-compose down

# Restart services
restart:
	@echo "Restarting Airflow environment..."
	docker-compose restart

# View logs
logs:
	docker-compose logs -f

# View webserver logs only
logs-webserver:
	docker-compose logs -f airflow-webserver

# View scheduler logs only
logs-scheduler:
	docker-compose logs -f airflow-scheduler

# Open shell in webserver container
shell:
	docker-compose exec airflow-webserver bash

# Trigger all DAGs for testing
trigger-all:
	@echo "Triggering all sample DAGs..."
	docker-compose exec airflow-webserver airflow dags trigger nyc_taxi_analytics
	docker-compose exec airflow-webserver airflow dags trigger weather_data_pipeline
	docker-compose exec airflow-webserver airflow dags trigger ecommerce_sales_etl
	docker-compose exec airflow-webserver airflow dags trigger stock_market_collector
	docker-compose exec airflow-webserver airflow dags trigger data_quality_checks
	docker-compose exec airflow-webserver airflow dags trigger social_media_analytics
	docker-compose exec airflow-webserver airflow dags trigger ml_training_pipeline
	@echo "All DAGs triggered!"

# Trigger social media DAG multiple times (high failure rate for testing)
trigger-failures:
	@echo "Triggering DAGs multiple times to generate failures..."
	@for i in 1 2 3 4 5; do \
		docker-compose exec airflow-webserver airflow dags trigger social_media_analytics; \
		docker-compose exec airflow-webserver airflow dags trigger weather_data_pipeline; \
		sleep 10; \
	done
	@echo "Done triggering DAGs"

# Check plugin status
check-plugin:
	docker-compose exec airflow-webserver airflow plugins

# Clean up everything
clean:
	@echo "Cleaning up Airflow environment..."
	docker-compose down -v --remove-orphans
	rm -rf logs/* data/*
	@echo "Cleanup complete"

# Initialize database (run after clean)
init-db:
	docker-compose up airflow-init

# Check container status
status:
	docker-compose ps
