# Airflow Watcher 👁️

An Airflow UI plugin for monitoring DAG failures and SLA misses/delays.

## Features

- 🚨 **DAG Failure Monitoring**: Real-time tracking of DAG and task failures
- ⏰ **SLA Miss Detection**: Alerts when DAGs miss their SLA deadlines
- 📊 **Dashboard View**: Custom Airflow UI view for monitoring status
- 🔔 **Multi-channel Notifications**: Support for Slack and email alerts
- 📈 **Trend Analysis**: Historical failure and SLA miss trends

## Installation

```bash
pip install -e .
```

Or for development:
```bash
pip install -e ".[dev]"
```

## Configuration

Add the following to your `airflow.cfg` or environment variables:

```ini
[airflow_watcher]
# Slack configuration
slack_webhook_url = https://hooks.slack.com/services/xxx/yyy/zzz
slack_channel = #airflow-alerts

# Email configuration
smtp_host = smtp.example.com
smtp_port = 587
alert_email_recipients = team@example.com

# Monitoring settings
failure_lookback_hours = 24
sla_check_interval_minutes = 5
```

## Usage

Once installed, the plugin will automatically:
1. Register with Airflow's plugin system
2. Add a "Watcher" menu item to the Airflow UI
3. Start monitoring DAG failures and SLA misses

### Custom Dashboard

Navigate to **Admin → Watcher Dashboard** in the Airflow UI to view:
- Recent DAG failures
- SLA misses and delays
- Failure trends over time
- DAG health status

## Project Structure

```
airflow-watcher/
├── src/
│   └── airflow_watcher/
│       ├── __init__.py
│       ├── plugins/           # Airflow plugin definitions
│       ├── views/             # Flask Blueprint views
│       ├── monitors/          # DAG & SLA monitoring logic
│       ├── notifiers/         # Slack, email notifications
│       ├── models/            # Data models
│       └── utils/             # Utilities and helpers
├── tests/
├── static/                    # CSS, JS for UI
├── templates/                 # Jinja2 templates
└── pyproject.toml
```

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run linting
ruff check src tests
black --check src tests

# Type checking
mypy src
```

## License

Apache License 2.0
