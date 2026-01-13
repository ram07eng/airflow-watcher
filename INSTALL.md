# Installation Guide

This guide covers all methods to install and configure the Airflow Watcher plugin.

## Table of Contents

- [Requirements](#requirements)
- [Installation Methods](#installation-methods)
  - [Method 1: pip install (Recommended)](#method-1-pip-install-recommended)
  - [Method 2: Copy to plugins folder](#method-2-copy-to-plugins-folder)
  - [Method 3: Docker](#method-3-docker)
- [Configuration](#configuration)
  - [Environment Variables](#environment-variables)
  - [airflow.cfg](#airflowcfg)
- [Verification](#verification)
- [Troubleshooting](#troubleshooting)

---

## Requirements

- **Python**: 3.9, 3.10, or 3.11
- **Apache Airflow**: 2.5.0 or higher
- **Database**: PostgreSQL (recommended) or MySQL

---

## Installation Methods

### Method 1: Clone and Install (Recommended)

Step-by-step installation after cloning:

```bash
# Step 1: Clone the repository
git clone https://github.com/ram07eng/airflow-watcher.git

# Step 2: Navigate to the project directory
cd airflow-watcher

# Step 3: Install the plugin
pip install -e .

# Step 4: Restart Airflow webserver
airflow webserver --daemon
# or if using docker
docker-compose restart airflow-webserver

# Step 5: Verify installation
airflow plugins
```

For development with additional tools (linting, testing):
```bash
pip install -e ".[dev]"
```

### Method 2: Direct pip install from GitHub

Install without cloning:

```bash
pip install git+https://github.com/ram07eng/airflow-watcher.git
```

### Method 3: Copy to plugins folder

Copy the plugin directly to your Airflow plugins directory:

```bash
# Clone the repository
git clone https://github.com/ram07eng/airflow-watcher.git

# Copy the plugin to Airflow plugins folder
cp -r airflow-watcher/src/airflow_watcher $AIRFLOW_HOME/plugins/

# Restart webserver
airflow webserver --daemon
```

Your plugins folder should look like:
```
$AIRFLOW_HOME/
├── dags/
├── logs/
└── plugins/
    └── airflow_watcher/
        ├── __init__.py
        ├── plugins/
        ├── views/
        ├── monitors/
        ├── notifiers/
        └── templates/
```

### Method 3: Docker

For Docker-based Airflow deployments, add to your Dockerfile:

```dockerfile
FROM apache/airflow:2.7.3-python3.10

# Install from GitHub
RUN pip install git+https://github.com/ram07eng/airflow-watcher.git

# Or copy local plugin
COPY --chown=airflow:root src/airflow_watcher /opt/airflow/plugins/airflow_watcher
```

Or mount as a volume in docker-compose.yml:

```yaml
services:
  airflow-webserver:
    image: apache/airflow:2.7.3-python3.10
    volumes:
      - ./airflow-watcher/src/airflow_watcher:/opt/airflow/plugins/airflow_watcher
```

---

## Configuration

The plugin can be configured via environment variables or `airflow.cfg`.

### Environment Variables

Set these environment variables in your deployment:

```bash
# Slack Notifications
AIRFLOW__WATCHER__SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz
AIRFLOW__WATCHER__SLACK_CHANNEL=#airflow-alerts

# Email Notifications
AIRFLOW__WATCHER__EMAIL_ENABLED=true
AIRFLOW__WATCHER__SMTP_HOST=smtp.example.com
AIRFLOW__WATCHER__SMTP_PORT=587
AIRFLOW__WATCHER__ALERT_EMAIL_RECIPIENTS=team@example.com,oncall@example.com

# Monitoring Settings
AIRFLOW__WATCHER__FAILURE_LOOKBACK_HOURS=24
AIRFLOW__WATCHER__SLA_CHECK_INTERVAL_MINUTES=5
AIRFLOW__WATCHER__STALE_DAG_THRESHOLD_HOURS=24
```

### airflow.cfg

Alternatively, add a `[watcher]` section to your `airflow.cfg`:

```ini
[watcher]
# Slack configuration
slack_webhook_url = https://hooks.slack.com/services/xxx/yyy/zzz
slack_channel = #airflow-alerts

# Email configuration
email_enabled = false
smtp_host = smtp.example.com
smtp_port = 587
alert_email_recipients = team@example.com

# Monitoring settings
failure_lookback_hours = 24
sla_check_interval_minutes = 5
stale_dag_threshold_hours = 24
```

### Configuration Options Reference

| Setting | Default | Description |
|---------|---------|-------------|
| `slack_webhook_url` | - | Slack webhook URL for alerts |
| `slack_channel` | `#airflow-alerts` | Slack channel name |
| `email_enabled` | `false` | Enable email notifications |
| `smtp_host` | - | SMTP server hostname |
| `smtp_port` | `587` | SMTP server port |
| `alert_email_recipients` | - | Comma-separated email list |
| `failure_lookback_hours` | `24` | Hours to look back for failures |
| `sla_check_interval_minutes` | `5` | SLA check frequency |
| `stale_dag_threshold_hours` | `24` | Hours before DAG is considered stale |

---

## Verification

After installation, verify the plugin is loaded:

1. **Restart Airflow webserver**:
   ```bash
   airflow webserver --daemon
   # or
   docker-compose restart airflow-webserver
   ```

2. **Check plugins list**:
   ```bash
   airflow plugins
   ```
   You should see `airflow_watcher` in the list.

3. **Access the UI**:
   - Navigate to http://localhost:8080
   - Look for **"Watcher"** in the top navigation menu

4. **Available Menu Items**:
   - **Airflow Dashboard** - Overview metrics
   - **Airflow Health** - DAG health status (success/failed/delayed/stale)
   - **DAG Scheduling** - Queue and pool utilization
   - **DAG Failures** - Recent failures with details
   - **SLA Tracker** - SLA misses and delays
   - **Task Health** - Long-running and zombie tasks
   - **Dependencies** - Cross-DAG dependency tracking

---

## Troubleshooting

### Plugin not appearing in menu

1. **Check plugin is in correct location**:
   ```bash
   ls -la $AIRFLOW_HOME/plugins/airflow_watcher/
   ```

2. **Check for import errors**:
   ```bash
   airflow plugins 2>&1 | grep -i error
   ```

3. **Verify Airflow logs**:
   ```bash
   tail -f $AIRFLOW_HOME/logs/webserver/*.log | grep -i watcher
   ```

### "naive datetime is disallowed" error

This error occurs when using `datetime.utcnow()` instead of Airflow's timezone-aware utilities.

The plugin uses `airflow.utils.timezone.utcnow()` to avoid this issue. If you see this error, ensure you have the latest version of the plugin.

### Database connection errors

Ensure your Airflow database is properly configured and accessible:

```bash
airflow db check
```

### Slack notifications not working

1. Test your webhook URL:
   ```bash
   curl -X POST -H 'Content-type: application/json' \
     --data '{"text":"Test from Airflow Watcher"}' \
     YOUR_WEBHOOK_URL
   ```

2. Verify environment variable is set:
   ```bash
   echo $AIRFLOW__WATCHER__SLACK_WEBHOOK_URL
   ```

---

## Uninstallation

To remove the plugin:

```bash
# If installed via pip
pip uninstall airflow-watcher

# If copied to plugins folder
rm -rf $AIRFLOW_HOME/plugins/airflow_watcher/
```

Then restart your Airflow webserver.

---

## Support

- **Issues**: https://github.com/ram07eng/airflow-watcher/issues
- **Documentation**: https://github.com/ram07eng/airflow-watcher
