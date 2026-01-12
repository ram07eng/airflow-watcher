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
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Build package
build: clean
	python -m build

# Start Docker environment
docker-up:
	docker-compose up -d --build
	@echo "Airflow UI available at http://localhost:8080"
	@echo "Username: admin, Password: admin"

# Stop Docker environment
docker-down:
	docker-compose down -v

# View Docker logs
docker-logs:
	docker-compose logs -f

# Help
help:
	@echo "Available commands:"
	@echo "  make install     - Install production dependencies"
	@echo "  make dev         - Install development dependencies"
	@echo "  make test        - Run tests"
	@echo "  make test-cov    - Run tests with coverage report"
	@echo "  make lint        - Run linting"
	@echo "  make lint-fix    - Fix linting issues"
	@echo "  make format      - Format code"
	@echo "  make format-check- Check code formatting"
	@echo "  make type-check  - Run type checking"
	@echo "  make check       - Run all checks"
	@echo "  make clean       - Clean build artifacts"
	@echo "  make build       - Build package"
	@echo "  make docker-up   - Start Docker environment"
	@echo "  make docker-down - Stop Docker environment"
