# Contributing to Airflow Watcher

Thanks for taking the time to contribute! Here's how to get started.

## Development Setup

```bash
# Clone the repo
git clone https://github.com/ram07eng/airflow-watcher.git
cd airflow-watcher

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dev dependencies
pip install -e ".[dev]"
```

## Running Tests

```bash
# Run all tests
pytest

# With coverage report
pytest --cov=airflow_watcher --cov-report=html

# Run a specific test file
pytest tests/test_dag_failure_monitor.py -v
```

## Code Quality

```bash
# Lint
ruff check src tests

# Auto-fix lint issues
ruff check --fix src tests

# Format
black src tests

# Type check
mypy src

# Run everything at once
make check
```

## Local Demo Environment

```bash
cd demo
docker-compose up -d
# Visit http://localhost:8080 (admin/admin)
```

## Project Structure

```
src/airflow_watcher/
├── alerting/       # Alert rules and manager
├── metrics/        # Prometheus & StatsD exporters
├── models/         # Data models (DAGFailure, SLAMissEvent, etc.)
├── monitors/       # Core monitoring logic
├── notifiers/      # Slack, Email, PagerDuty
├── plugins/        # Airflow plugin registration
├── templates/      # Jinja2 HTML templates
├── utils/          # Helpers
└── views/          # Flask views and REST API
```

## Submitting a Pull Request

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes and add tests
4. Ensure all checks pass: `make check`
5. Commit with a clear message: `git commit -m "feat: add X"`
6. Push and open a PR against `main`

## Commit Message Convention

We follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` new feature
- `fix:` bug fix
- `docs:` documentation only
- `test:` adding or updating tests
- `refactor:` code change that neither fixes a bug nor adds a feature
- `chore:` build process or tooling changes

## Reporting Issues

Please use [GitHub Issues](https://github.com/ram07eng/airflow-watcher/issues) and include:
- Airflow version
- Python version
- Steps to reproduce
- Expected vs actual behaviour
