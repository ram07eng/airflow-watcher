# Airflow Watcher 👁️

An Airflow UI plugin for monitoring DAG failures and SLA misses/delays.

## Demo

![Airflow Watcher Demo](https://raw.githubusercontent.com/ram07eng/airflow-watcher/main/docs/demo.gif)

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

## Architecture

```
+--------------------------------------------------------------+
|                   Airflow Webserver                          |
|                                                              |
|  +--------------------------------------------------------+  |
|  |              Airflow Watcher Plugin                    |  |
|  |                                                        |  |
|  |  +-------------+     +------------------------------+  |  |
|  |  | Flask Views  |    |        Monitors (6)          |  |  |
|  |  | (Dashboard)  |<---|  - DAG Failure Monitor       |  |  |
|  |  |              |    |  - SLA Monitor               |  |  |
|  |  | REST API     |    |  - Task Health Monitor       |  |  |
|  |  | /api/watcher |    |  - Scheduling Monitor        |  |  |
|  |  +-------------+     |  - Dependency Monitor        |  |  |
|  |         |            |  - DAG Health Monitor        |  |  |
|  |         |            +----------+-------------------+  |  |
|  |         |                      |                       |  |
|  |         |           +----------v-------------------+   |  |
|  |         |           |    Metrics Collector          |  |  |
|  |         |           |    (WatcherMetrics)           |  |  |
|  |         |           +----------+-------------------+   |  |
|  |         |                      |                       |  |
|  |         v                      v                       |  |
|  |  +-------------+     +------------------------------+  |  |
|  |  |  Notifiers   |    |        Emitters              |  |  |
|  |  |  - Slack     |    |  - StatsD / Datadog (UDP)    |  |  |
|  |  |  - Email     |    |  - Prometheus (/metrics)     |  |  |
|  |  |  - PagerDuty |    |                              |  |  |
|  |  +-------------+     +------------------------------+  |  |
|  +--------------------------------------------------------+  |
|                          |                                   |
|                          v                                   |
|              +-----------------------+                       |
|              |  Airflow Metadata DB  |                       |
|              |  (PostgreSQL/MySQL)   |                       |
|              +-----------------------+                       |
+--------------------------------------------------------------+
```

Everything runs inside the Airflow webserver process. No separate workers, no message queues, no external databases. The plugin reads from the same metadata DB that Airflow already maintains.

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

## MWAA (Amazon Managed Workflows for Apache Airflow)

### Setup

1. Add `airflow-watcher` to your MWAA `requirements.txt`:

```
airflow-watcher==0.1.2
```

For Prometheus metrics support:
```
airflow-watcher[all]==0.1.2
```

2. Upload `requirements.txt` to your MWAA S3 bucket:

```bash
aws s3 cp requirements.txt s3://<your-mwaa-bucket>/requirements.txt
```

3. Update your MWAA environment to pick up the new requirements (via AWS Console or CLI):

```bash
aws mwaa update-environment \
  --name <your-environment-name> \
  --requirements-s3-path requirements.txt \
  --requirements-s3-object-version <version-id>
```

> **Note:** No `plugins.zip` is needed. Airflow auto-discovers airflow-watcher via the `airflow.plugins` entry point when installed via pip (Airflow 2.7+).

4. Wait for the environment to finish updating (takes a few minutes).

5. Verify at:
```
https://<your-mwaa-url>/api/watcher/health
```

### Environment Variables (optional)

Configure via MWAA Airflow configuration overrides:

| Variable | Purpose |
|---|---|
| `AIRFLOW_WATCHER__SLACK_WEBHOOK_URL` | Slack notifications |
| `AIRFLOW_WATCHER__PAGERDUTY_API_KEY` | PagerDuty alerts |
| `AIRFLOW_WATCHER__ENABLE_PROMETHEUS` | Prometheus metrics |

### Testing Locally with MWAA Local Runner

```bash
git clone https://github.com/aws/aws-mwaa-local-runner.git
cd aws-mwaa-local-runner
echo "airflow-watcher==0.1.2" >> requirements/requirements.txt
./mwaa-local-env build-image
./mwaa-local-env start
```

Visit `http://localhost:8080/api/watcher/health` to verify.

> **Note:** If using Slack or PagerDuty notifications, ensure your MWAA VPC has a NAT gateway for outbound internet access.

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
