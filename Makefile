.PHONY: install dev test lint format type-check clean build docker-up docker-down

# Install production dependencies
install:
	pip install -e .

# Install development dependencies
dev:
	pip install -e ".[dev]"

# Run tests
test:
	pytest tests/ -v --cov=airflow_watcher --cov-report=term-missing

# Run tests with coverage report
test-cov:
	pytest tests/ -v --cov=airflow_watcher --cov-report=html
	@echo "Coverage report: htmlcov/index.html"

# Run linting
lint:
	ruff check src tests

# Fix linting issues
lint-fix:
	ruff check --fix src tests

# Format code
format:
	black src tests

# Check formatting
format-check:
	black --check src tests

# Type checking
type-check:
	mypy src

# Run all checks
check: lint format-check type-check test

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf logs/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Build package
build: clean
	python -m build

# Start Docker environment
docker-up:
	mkdir -p logs
	docker-compose up -d --build
	@echo ""
	@echo "============================================"
	@echo "Airflow is starting up..."
	@echo "============================================"
	@echo ""
	@echo "Wait ~60 seconds for initialization, then:"
	@echo ""
	@echo "  Airflow UI:    http://localhost:8080"
	@echo "  Username:      admin"
	@echo "  Password:      admin"
	@echo ""
	@echo "  Watcher Views:"
	@echo "    Dashboard:    http://localhost:8080/watcher/dashboard"
	@echo "    Failures:     http://localhost:8080/watcher/failures"
	@echo "    SLA:          http://localhost:8080/watcher/sla"
	@echo "    Task Health:  http://localhost:8080/watcher/tasks"
	@echo "    Scheduling:   http://localhost:8080/watcher/scheduling"
	@echo "    DAG Health:   http://localhost:8080/watcher/dag-health"
	@echo "    Dependencies: http://localhost:8080/watcher/dependencies"
	@echo ""
	@echo "  API Endpoints:"
	@echo "    Health:       http://localhost:8080/api/watcher/health"
	@echo "    Failures:     http://localhost:8080/api/watcher/failures"
	@echo ""
	@echo "Run 'make docker-logs' to view logs"
	@echo "Run 'make docker-down' to stop"
	@echo "============================================"

# Stop Docker environment
docker-down:
	docker-compose down -v
	@echo "Airflow stopped and volumes removed."

# View Docker logs
docker-logs:
	docker-compose logs -f

# Restart Docker environment
docker-restart: docker-down docker-up

# Check container status
docker-status:
	docker-compose ps

# Trigger sample DAGs for testing
trigger-dags:
	@echo "Triggering sample DAGs for testing..."
	docker-compose exec airflow-webserver airflow dags unpause nyc_taxi_etl
	docker-compose exec airflow-webserver airflow dags unpause weather_data_pipeline
	docker-compose exec airflow-webserver airflow dags unpause driver_analytics
	docker-compose exec airflow-webserver airflow dags unpause payment_processing
	docker-compose exec airflow-webserver airflow dags unpause demand_forecasting
	docker-compose exec airflow-webserver airflow dags unpause test_failure_dag
	docker-compose exec airflow-webserver airflow dags unpause sla_breach_test
	docker-compose exec airflow-webserver airflow dags trigger nyc_taxi_etl
	docker-compose exec airflow-webserver airflow dags trigger weather_data_pipeline
	docker-compose exec airflow-webserver airflow dags trigger test_failure_dag
	docker-compose exec airflow-webserver airflow dags trigger sla_breach_test
	@echo "DAGs triggered! Check http://localhost:8080 for results"

# Help
help:
	@echo "Available commands:"
	@echo ""
	@echo "Development:"
	@echo "  make install      - Install production dependencies"
	@echo "  make dev          - Install development dependencies"
	@echo "  make test         - Run tests"
	@echo "  make test-cov     - Run tests with coverage report"
	@echo "  make lint         - Run linting"
	@echo "  make lint-fix     - Fix linting issues"
	@echo "  make format       - Format code"
	@echo "  make format-check - Check code formatting"
	@echo "  make type-check   - Run type checking"
	@echo "  make check        - Run all checks"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make build        - Build package"
	@echo ""
	@echo "Docker (Local Testing):"
	@echo "  make docker-up      - Start Airflow with sample DAGs"
	@echo "  make docker-down    - Stop Airflow and remove volumes"
	@echo "  make docker-restart - Restart Airflow"
	@echo "  make docker-logs    - View container logs"
	@echo "  make docker-status  - Check container status"
	@echo "  make trigger-dags   - Trigger sample DAGs for testing"
