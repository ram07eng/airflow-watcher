# Airflow Watcher 👁️

An Airflow UI plugin for monitoring DAG failures and SLA misses/delays.

## Demo

![Airflow Watcher Demo](https://raw.githubusercontent.com/ram07eng/airflow-watcher/main/docs/images/demo_new.gif)

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

## Role-Based Access Control (RBAC)

Airflow Watcher integrates with Airflow's built-in FAB security manager to enforce DAG-level access control. No separate configuration is needed — it reads directly from Airflow's role and permission system.

### How It Works

- **Admin / Op roles** see all DAGs across every Watcher page and API endpoint
- **Custom roles** only see DAGs they have `can_read` permission on
- Filtering is mandatory and applied server-side — restricted users cannot bypass it
- Aggregate stats (failure counts, SLA misses, health scores) are recomputed per-user so no global data leaks
- A 🔒 badge appears in the filter bar for non-admin users

### Setting Up DAG-Level Permissions

Add `access_control` to your DAG definitions to grant team-specific access:

```python
from airflow import DAG

dag = DAG(
    dag_id="weather_data_pipeline",
    schedule_interval="@hourly",
    access_control={
        "team_weather": {"can_read", "can_edit"},
    },
)
```

Then create matching roles in Airflow (Admin → Security → List Roles) and assign users to them. The Watcher plugin will automatically pick up the permissions.

### What Gets Filtered

| Area | Filtering |
|------|-----------|
| Dashboard stats | Failure count, SLA misses, health score — all scoped to user's DAGs |
| Failures page | Only failures from accessible DAGs |
| SLA page | Only SLA misses from accessible DAGs |
| Health page | Health status, stale DAGs, scheduling lag — filtered |
| Task health | Long-running tasks, zombies, retries — filtered |
| Scheduling | Concurrent runs, delayed DAGs — filtered |
| Dependencies | Cross-DAG deps, correlations — filtered |
| All API endpoints | Same RBAC enforcement as UI pages |

### Demo Users

The demo environment includes pre-configured RBAC users:

| User | Role | Visible DAGs |
|------|------|-------------|
| `admin` | Admin | All 8 DAGs |
| `weather_user` | team_weather | weather_data_pipeline, stock_market_collector |
| `ecommerce_user` | team_ecommerce | ecommerce_sales_etl, data_quality_checks |

Passwords are configured in `demo/docker-compose.yml`. Change them before any shared deployment.

### RBAC Demo

**Admin user** — sees all DAGs and full aggregate stats:

![Admin RBAC Demo](https://raw.githubusercontent.com/ram07eng/airflow-watcher/main/docs/images/rbac_admin.gif)

**Weather team user** — only sees weather_data_pipeline and stock_market_collector:

![Weather User RBAC Demo](https://raw.githubusercontent.com/ram07eng/airflow-watcher/main/docs/images/rbac_weather.gif)

**Ecommerce team user** — only sees ecommerce_sales_etl and data_quality_checks:

![Ecommerce User RBAC Demo](https://raw.githubusercontent.com/ram07eng/airflow-watcher/main/docs/images/rbac_ecommerce.gif)

```bash
cd demo
docker-compose up -d
# Visit http://localhost:8080 and login as any user above
```

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

### Plugin API Endpoints — `/api/watcher`

The plugin exposes a REST API at `/api/watcher/*` on the Airflow webserver.
Authentication uses Airflow session cookies (same login as the UI).
All endpoints return JSON with a standard `{status, data, timestamp}` envelope.

| Category | Method | Path | Description |
|----------|--------|------|-------------|
| **Failures** | GET | `/api/watcher/failures` | Recent DAG failures |
| | GET | `/api/watcher/failures/stats` | Failure rate statistics |
| **SLA** | GET | `/api/watcher/sla/misses` | SLA miss events |
| | GET | `/api/watcher/sla/stats` | SLA miss statistics |
| **Health** | GET | `/api/watcher/health` | System health (200/503) |
| | GET | `/api/watcher/health/<dag_id>` | Per-DAG health |
| **Tasks** | GET | `/api/watcher/tasks/long-running` | Tasks exceeding threshold |
| | GET | `/api/watcher/tasks/retries` | Tasks with excessive retries |
| | GET | `/api/watcher/tasks/zombies` | Potential zombie tasks |
| | GET | `/api/watcher/tasks/failure-patterns` | Failure pattern analysis |
| **Scheduling** | GET | `/api/watcher/scheduling/lag` | Scheduling delay percentiles |
| | GET | `/api/watcher/scheduling/queue` | Current queue status |
| | GET | `/api/watcher/scheduling/pools` | Pool utilization |
| | GET | `/api/watcher/scheduling/stale-dags` | DAGs not running on schedule |
| | GET | `/api/watcher/scheduling/concurrent` | Concurrent DAG runs |
| **DAGs** | GET | `/api/watcher/dags/import-errors` | DAG import errors |
| | GET | `/api/watcher/dags/status-summary` | Status summary with health score |
| | GET | `/api/watcher/dags/complexity` | DAG complexity analysis |
| | GET | `/api/watcher/dags/inactive` | Inactive DAGs |
| **Dependencies** | GET | `/api/watcher/dependencies/upstream-failures` | Upstream failure cascades |
| | GET | `/api/watcher/dependencies/cross-dag` | Cross-DAG dependencies |
| | GET | `/api/watcher/dependencies/correlations` | Failure correlations |
| | GET | `/api/watcher/dependencies/impact/<dag_id>/<task_id>` | Downstream impact |
| **Overview** | GET | `/api/watcher/overview` | Combined overview |

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
│       ├── templates/         # Jinja2 templates
│       └── api/               # Standalone FastAPI service
├── demo/                      # Local demo Airflow environment
│   ├── dags/                  # Sample DAGs for testing
│   ├── plugins/               # Plugin copy for demo
│   └── docker-compose.yml     # Docker setup
├── tests/                     # 302 unit tests
│   └── live/                  # Live integration tests (need Docker)
│       ├── test_qa_deep.py    # Standalone API deep QA (334 tests)
│       ├── test_qa_plugin.py  # Plugin API deep QA (138 tests)
│       └── ...
├── docs/screenshots/          # Dashboard screenshots
└── pyproject.toml
```

## Demo Environment

To test the plugin locally with sample DAGs:

```bash
cd demo
docker-compose up -d
```

Then visit http://localhost:8080 and navigate to the **Watcher** menu.

See [demo/README.md](demo/README.md) for more details.

<details>
<summary><h2>MWAA Integration</h2></summary>

### Setup

1. Add `airflow-watcher` to your MWAA `requirements.txt`:

```
airflow-watcher==1.0.0
```

For Prometheus metrics support:
```
airflow-watcher[all]==1.0.0
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
echo "airflow-watcher==1.0.0" >> requirements/requirements.txt
./mwaa-local-env build-image
./mwaa-local-env start
```

Visit `http://localhost:8080/api/watcher/health` to verify.

> **Note:** If using Slack or PagerDuty notifications, ensure your MWAA VPC has a NAT gateway for outbound internet access.

</details>

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Unit tests (302 pass)
pytest tests/ --ignore=tests/live -v --no-cov

# Security & penetration tests only
pytest tests/test_security.py -v

# Load & stress tests only
pytest tests/test_load.py -v

# Live integration tests (requires demo Docker environment)
python tests/live/test_qa_deep.py      # Standalone API deep QA (334 tests)
python tests/live/test_qa_plugin.py     # Plugin API deep QA (138 tests)

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
