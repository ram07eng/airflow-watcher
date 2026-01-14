# Alerting & Monitoring Configuration

This guide covers how to configure alerting and monitoring for Airflow Watcher.

## Table of Contents

- [Overview](#overview)
- [Alert Channels](#alert-channels)
  - [Slack](#slack)
  - [Email](#email)
  - [PagerDuty](#pagerduty)
- [Metrics Export](#metrics-export)
  - [StatsD / Datadog](#statsd--datadog)
  - [Prometheus](#prometheus)
- [Alert Rules](#alert-rules)
  - [Pre-defined Templates](#pre-defined-templates)
  - [Custom Rules](#custom-rules)
- [Configuration Reference](#configuration-reference)

---

## Overview

Airflow Watcher supports multiple alerting channels and metrics backends:

| Feature | Description |
|---------|-------------|
| **Slack** | Rich notifications with blocks and buttons |
| **Email** | SMTP-based email alerts |
| **PagerDuty** | Incident management with deduplication |
| **StatsD** | Real-time metrics to StatsD/Datadog |
| **Prometheus** | `/metrics` endpoint for scraping |

---

## Alert Channels

### Slack

Configure Slack alerts using either a webhook URL or bot token.

**Option 1: Webhook URL (Recommended)**

```bash
# Environment variables
export AIRFLOW_WATCHER_SLACK_WEBHOOK_URL="https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"
export AIRFLOW_WATCHER_SLACK_CHANNEL="#airflow-alerts"
```

**Option 2: Bot Token**

```bash
export AIRFLOW_WATCHER_SLACK_TOKEN="xoxb-your-bot-token"
export AIRFLOW_WATCHER_SLACK_CHANNEL="#airflow-alerts"
```

**airflow.cfg:**

```ini
[airflow_watcher]
slack_webhook_url = https://hooks.slack.com/services/...
slack_channel = #airflow-alerts
```

### Email

Configure SMTP for email alerts.

```bash
# Environment variables
export AIRFLOW_WATCHER_SMTP_HOST="smtp.gmail.com"
export AIRFLOW_WATCHER_SMTP_PORT="587"
export AIRFLOW_WATCHER_SMTP_USER="your-email@gmail.com"
export AIRFLOW_WATCHER_SMTP_PASSWORD="your-app-password"
export AIRFLOW_WATCHER_EMAIL_FROM="airflow-alerts@yourcompany.com"
export AIRFLOW_WATCHER_EMAIL_RECIPIENTS="oncall@yourcompany.com,team@yourcompany.com"
```

**airflow.cfg:**

```ini
[airflow_watcher]
smtp_host = smtp.gmail.com
smtp_port = 587
smtp_use_tls = true
smtp_user = your-email@gmail.com
smtp_password = your-app-password
email_from = airflow-alerts@yourcompany.com
email_recipients = oncall@yourcompany.com,team@yourcompany.com
```

### PagerDuty

Configure PagerDuty for incident management.

1. **Create a PagerDuty Integration:**
   - Go to your PagerDuty service
   - Add integration → Events API v2
   - Copy the Integration Key (Routing Key)

2. **Configure:**

```bash
export AIRFLOW_WATCHER_PAGERDUTY_ROUTING_KEY="your-integration-key"
export AIRFLOW_WATCHER_PAGERDUTY_SERVICE_NAME="Airflow Production"
```

**airflow.cfg:**

```ini
[airflow_watcher]
pagerduty_routing_key = your-integration-key
pagerduty_service_name = Airflow Production
```

**Features:**
- Automatic alert deduplication
- Resolve alerts when issues are fixed
- Custom severity levels (info, warning, error, critical)

---

## Metrics Export

### StatsD / Datadog

Export metrics to StatsD or DogStatsD (Datadog).

```bash
export AIRFLOW_WATCHER_STATSD_ENABLED="true"
export AIRFLOW_WATCHER_STATSD_HOST="localhost"
export AIRFLOW_WATCHER_STATSD_PORT="8125"
export AIRFLOW_WATCHER_STATSD_PREFIX="airflow.watcher"

# For Datadog (DogStatsD format with tags)
export AIRFLOW_WATCHER_USE_DOGSTATSD="true"
```

**Metrics emitted:**

| Metric | Type | Description |
|--------|------|-------------|
| `airflow.watcher.failures.total_24h` | gauge | Total failures in 24h |
| `airflow.watcher.failures.unique_dags_24h` | gauge | Unique DAGs failed |
| `airflow.watcher.sla.misses_24h` | gauge | SLA misses in 24h |
| `airflow.watcher.dags.unhealthy` | gauge | Unhealthy DAG count |
| `airflow.watcher.dags.healthy_percent` | gauge | Health percentage |
| `airflow.watcher.tasks.long_running` | gauge | Long-running tasks |

### Prometheus

Expose metrics for Prometheus scraping.

```bash
export AIRFLOW_WATCHER_PROMETHEUS_ENABLED="true"
export AIRFLOW_WATCHER_PROMETHEUS_PREFIX="airflow_watcher"
```

The plugin exposes a `/watcher/metrics` endpoint in Prometheus format.

**Prometheus scrape config:**

```yaml
scrape_configs:
  - job_name: 'airflow-watcher'
    static_configs:
      - targets: ['airflow-webserver:8080']
    metrics_path: '/watcher/metrics'
```

---

## Alert Rules

### Pre-defined Templates

Choose a template that matches your environment:

```bash
export AIRFLOW_WATCHER_ALERT_TEMPLATE="production_balanced"
```

| Template | Description | Use Case |
|----------|-------------|----------|
| `production_strict` | Alert on any failure | Critical production systems |
| `production_balanced` | Alert on patterns | Standard production |
| `production_relaxed` | Alert on high volumes | Tolerant environments |
| `development` | Minimal alerts | Dev/staging |

**Template Details:**

**production_strict:**
- Alert on any DAG failure → Slack + PagerDuty
- Alert on any SLA miss → Slack
- Alert if health < 90% → Slack + PagerDuty + Email

**production_balanced:**
- Alert on 5+ failures → Slack
- Alert on 10+ failures → Slack + PagerDuty
- Alert if health < 80% → Slack + PagerDuty

**production_relaxed:**
- Alert on 20+ failures → Slack
- Alert on 50+ failures → Slack + PagerDuty
- Alert if health < 60% → Slack + PagerDuty

### Custom Rules

**Option 1: Environment Variables**

```bash
export WATCHER_ALERT_RULE_1='{"name":"custom_failure","metric":"failures.total_24h","condition":"gte","threshold":5,"severity":"warning","channels":["slack"]}'
export WATCHER_ALERT_RULE_2='{"name":"custom_sla","metric":"sla.misses_24h","condition":"gte","threshold":3,"severity":"error","channels":["slack","pagerduty"]}'
```

**Option 2: JSON File**

Create `/etc/airflow/watcher_rules.json`:

```json
[
  {
    "name": "critical_failures",
    "metric": "failures.total_24h",
    "condition": "gte",
    "threshold": 10,
    "severity": "critical",
    "channels": ["slack", "pagerduty", "email"],
    "cooldown_minutes": 15
  },
  {
    "name": "sla_warnings",
    "metric": "sla.misses_24h",
    "condition": "gte",
    "threshold": 5,
    "severity": "warning",
    "channels": ["slack"],
    "cooldown_minutes": 30
  },
  {
    "name": "low_health",
    "metric": "dags.healthy_percent",
    "condition": "lt",
    "threshold": 75,
    "severity": "error",
    "channels": ["slack", "pagerduty"],
    "cooldown_minutes": 15
  }
]
```

Then configure:

```bash
export AIRFLOW_WATCHER_ALERT_RULES_FILE="/etc/airflow/watcher_rules.json"
```

**Option 3: Python Code**

```python
from airflow_watcher.alerting import AlertManager, AlertChannel, AlertSeverity
from airflow_watcher.alerting.rules import create_custom_rule

# Create alert manager
manager = AlertManager()

# Add custom rule
rule = create_custom_rule(
    name="my_custom_rule",
    metric="failures.total_24h",
    condition="gte",
    threshold=15,
    severity="error",
    channels=["slack", "pagerduty"],
    cooldown_minutes=10,
    dag_filter="prod_.*",  # Only match DAGs starting with prod_
)
manager.add_rule(rule)
```

### Available Metrics

| Metric | Description |
|--------|-------------|
| `failures.total_24h` | Total DAG failures in last 24 hours |
| `failures.unique_dags_24h` | Unique DAGs that failed |
| `failures.rate_percent` | Failure rate as percentage |
| `sla.misses_24h` | SLA misses in last 24 hours |
| `tasks.failed_24h` | Failed tasks in last 24 hours |
| `tasks.retry_24h` | Tasks pending retry |
| `tasks.long_running` | Long-running tasks (>1 hour) |
| `scheduling.missed_24h` | Missed schedules |
| `scheduling.delayed_dags` | DAGs with scheduling delays |
| `dags.unhealthy` | Number of unhealthy DAGs |
| `dags.total` | Total DAG count |
| `dags.healthy_percent` | Percentage of healthy DAGs |
| `dependencies.failed_sensors` | Failed sensor tasks |
| `dependencies.blocked_dags` | Blocked DAGs |

### Rule Conditions

| Condition | Meaning |
|-----------|---------|
| `gt` | Greater than |
| `gte` | Greater than or equal |
| `lt` | Less than |
| `lte` | Less than or equal |
| `eq` | Equal to |

---

## Configuration Reference

### All Environment Variables

```bash
# Slack
AIRFLOW_WATCHER_SLACK_WEBHOOK_URL=
AIRFLOW_WATCHER_SLACK_TOKEN=
AIRFLOW_WATCHER_SLACK_CHANNEL=

# Email
AIRFLOW_WATCHER_SMTP_HOST=
AIRFLOW_WATCHER_SMTP_PORT=
AIRFLOW_WATCHER_SMTP_USER=
AIRFLOW_WATCHER_SMTP_PASSWORD=
AIRFLOW_WATCHER_EMAIL_FROM=
AIRFLOW_WATCHER_EMAIL_RECIPIENTS=

# PagerDuty
AIRFLOW_WATCHER_PAGERDUTY_ROUTING_KEY=
AIRFLOW_WATCHER_PAGERDUTY_SERVICE_NAME=

# StatsD
AIRFLOW_WATCHER_STATSD_ENABLED=
AIRFLOW_WATCHER_STATSD_HOST=
AIRFLOW_WATCHER_STATSD_PORT=
AIRFLOW_WATCHER_STATSD_PREFIX=
AIRFLOW_WATCHER_USE_DOGSTATSD=

# Prometheus
AIRFLOW_WATCHER_PROMETHEUS_ENABLED=
AIRFLOW_WATCHER_PROMETHEUS_PREFIX=

# Alerting
AIRFLOW_WATCHER_ALERT_TEMPLATE=
AIRFLOW_WATCHER_ALERT_RULES_FILE=
AIRFLOW_WATCHER_BASE_URL=

# Individual rules (JSON format)
WATCHER_ALERT_RULE_1=
WATCHER_ALERT_RULE_2=
# ... etc
```

### Complete airflow.cfg Section

```ini
[airflow_watcher]
# Slack
slack_webhook_url = 
slack_token = 
slack_channel = #airflow-alerts

# Email
smtp_host = 
smtp_port = 587
smtp_use_tls = true
smtp_user = 
smtp_password = 
email_from = airflow-watcher@example.com
email_recipients = 

# PagerDuty
pagerduty_routing_key = 
pagerduty_service_name = Airflow Watcher

# StatsD
statsd_enabled = false
statsd_host = localhost
statsd_port = 8125
statsd_prefix = airflow.watcher
use_dogstatsd = false

# Prometheus
prometheus_enabled = false
prometheus_prefix = airflow_watcher

# Alert settings
alert_on_first_failure = true
alert_on_retry_failure = false
batch_alerts = false
batch_interval_minutes = 15

# Monitoring settings
failure_lookback_hours = 24
sla_check_interval_minutes = 5
sla_warning_threshold_minutes = 30

# General
airflow_base_url = http://localhost:8080
alert_rules_file = 
alert_template = production_balanced
```

---

## Testing Alerts

Test your alert configuration:

```python
from airflow_watcher.alerting import AlertManager, AlertChannel

manager = AlertManager()

# Check configured channels
print("Configured channels:", manager.get_configured_channels())

# Test Slack
if AlertChannel.SLACK in manager.get_configured_channels():
    success = manager.test_channel(AlertChannel.SLACK)
    print(f"Slack test: {'✅ Success' if success else '❌ Failed'}")

# Test PagerDuty
if AlertChannel.PAGERDUTY in manager.get_configured_channels():
    success = manager.test_channel(AlertChannel.PAGERDUTY)
    print(f"PagerDuty test: {'✅ Success' if success else '❌ Failed'}")
```

Or use the CLI (coming soon):

```bash
airflow watcher test-alerts --channel slack
airflow watcher test-alerts --channel pagerduty
airflow watcher test-alerts --all
```
