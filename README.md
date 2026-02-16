# Airflow Watcher 👁️

An Airflow UI plugin for monitoring DAG failures and SLA misses/delays.

## Demo

![Airflow Watcher Demo](docs/demo.gif)

## Features

- 🚨 **DAG Failure Monitoring**: Real-time tracking of DAG and task failures
- ⏰ **SLA Miss Detection**: Alerts when DAGs miss their SLA deadlines
- 📊 **Dashboard View**: Custom Airflow UI view for monitoring status
- 🔔 **Multi-channel Notifications**: Slack, Email, and PagerDuty alerts
- 📈 **Trend Analysis**: Historical failure and SLA miss trends
- 📡 **Metrics Export**: StatsD/Datadog and Prometheus support
- ⚙️ **Flexible Alert Rules**: Pre-defined templates or custom rules

## Installation

📖 **See [INSTALL.md](INSTALL.md) for detailed installation and configuration instructions.**

## Alerting & Monitoring

📖 **See [ALERTING.md](ALERTING.md) for complete alerting configuration:**

- **Slack** - Rich notifications with blocks
- **Email** - SMTP-based alerts
- **PagerDuty** - Incident management with deduplication
- **StatsD/Datadog** - Real-time metrics
- **Prometheus** - `/metrics` endpoint for scraping

### Quick Setup

```bash
# Slack alerts
export AIRFLOW_WATCHER_SLACK_WEBHOOK_URL="https://hooks.slack.com/..."

# PagerDuty (optional)
export AIRFLOW_WATCHER_PAGERDUTY_ROUTING_KEY="your-key"

# Choose alert template
export AIRFLOW_WATCHER_ALERT_TEMPLATE="production_balanced"
```

## Usage

Once installed, the plugin will automatically:
1. Register with Airflow's plugin system
2. Add a "Watcher" menu item to the Airflow UI
3. Start monitoring DAG failures and SLA misses

### Watcher Menu

Navigate to **Watcher** in the Airflow UI navigation to access:
- **Airflow Dashboard** - Overview metrics
- **Airflow Health** - DAG health status (success/failed/delayed/stale)
- **DAG Scheduling** - Queue and pool utilization
- **DAG Failures** - Recent failures with details
- **SLA Tracker** - SLA misses and delays
- **Task Health** - Long-running and zombie tasks
- **Dependencies** - Cross-DAG dependency tracking

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
│       └── templates/         # Jinja2 templates
├── demo/                      # Local demo Airflow environment
│   ├── dags/                  # Sample DAGs for testing
│   ├── plugins/               # Plugin copy for demo
│   └── docker-compose.yml     # Docker setup
├── tests/
└── pyproject.toml
```

## Demo Environment

To test the plugin locally with sample DAGs:

```bash
cd demo
docker-compose up -d
```

Then visit http://localhost:8080 (admin/admin) and navigate to the **Watcher** menu.

See [demo/README.md](demo/README.md) for more details.

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

Apache License 2.0 - See [LICENSE](LICENSE) for details.

## Author

**Ramanujam Solaimalai** ([@ram07eng](https://github.com/ram07eng))

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request
