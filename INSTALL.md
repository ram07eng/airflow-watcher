# Installation Guide

This guide covers all methods to install and configure both the Airflow Watcher **plugin** (embedded in the Airflow webserver) and the **standalone REST API** (separate FastAPI process).

| Component | Runs in | Auth | Use case |
|-----------|---------|------|----------|
| **[Plugin](#plugin-installation)** | Airflow webserver process | Airflow session cookies + FAB RBAC | UI dashboards, internal API at `/api/watcher` |
| **[Standalone API](#standalone-api-installation)** | Separate FastAPI process | Bearer token + per-key RBAC | External integrations, CI/CD, dashboards, automation |

## Table of Contents

- [Requirements](#requirements)
- **Plugin**
  - [Plugin Installation](#plugin-installation)
    - [Method 1: pip install (Recommended)](#method-1-pip-install-recommended)
    - [Method 2: pip install from GitHub](#method-2-pip-install-from-github)
    - [Method 3: Clone and Install](#method-3-clone-and-install)
    - [Method 4: Add to requirements.txt](#method-4-add-to-requirementstxt)
    - [Method 5: Copy to plugins folder](#method-5-copy-to-plugins-folder)
    - [Method 6: Docker](#method-6-docker)
  - [Plugin Configuration](#plugin-configuration)
    - [Environment Variables](#environment-variables)
    - [airflow.cfg](#airflowcfg)
    - [Configuration Options Reference](#configuration-options-reference)
  - [Plugin Verification](#plugin-verification)
- **Standalone API**
  - [Standalone API Installation](#standalone-api-installation)
  - [API Quick Start](#api-quick-start)
  - [API Authentication](#api-authentication)
  - [API Configuration Reference](#api-configuration-reference)
  - [API RBAC (Per-Key DAG Filtering)](#api-rbac-per-key-dag-filtering)
  - [Connecting to the Metadata Database](#connecting-to-the-metadata-database)
  - [API Verification](#api-verification)
- **Infrastructure Integration**
  - [AWS MWAA](#aws-mwaa)
  - [Google Cloud Composer](#google-cloud-composer)
  - [Kubernetes / Helm](#kubernetes--helm-airflow-helm-chart)
  - [Production Deployment (Standalone API)](#production-deployment-standalone-api)
- [Upgrading & Migration](#upgrading--migration)
- [Troubleshooting](#troubleshooting)
- [Uninstallation](#uninstallation)

---

## Requirements

- **Python**: 3.10, 3.11, or 3.12
- **Apache Airflow**: 2.7.0–2.10.x (Airflow 3.x is **not yet supported** — see [Airflow Version Compatibility](README.md#airflow-version-compatibility))
- **Database**: PostgreSQL (recommended) or MySQL

### Requirements Files

The repository includes two requirements files for different deployment targets:

| File | Purpose | Install command |
|------|---------|-----------------|
| `requirements.txt` | Plugin dependencies (Airflow, Flask, Slack SDK) | `pip install -r requirements.txt` |
| `requirements-api.txt` | Standalone API dependencies (includes FastAPI, Uvicorn, psycopg2) | `pip install -r requirements-api.txt` |

> **Note:** `requirements-api.txt` is a superset — it includes everything from `requirements.txt` plus the standalone API packages.

---

# Plugin Installation

## Installation Methods

### Method 1: pip install from PyPI (Recommended)

Install the latest stable version from PyPI:

```bash
pip install airflow-watcher
```

To install a specific version:

```bash
pip install airflow-watcher==1.1.0
```

### Method 2: pip install from GitHub

Install directly from GitHub without cloning:

```bash
# Install latest version
pip install git+https://github.com/ram07eng/airflow-watcher.git

# Install specific version/tag
pip install git+https://github.com/ram07eng/airflow-watcher.git@v1.1.0

# Install with development dependencies
pip install "airflow-watcher[dev] @ git+https://github.com/ram07eng/airflow-watcher.git"
```

### Method 3: Clone and Install

Step-by-step installation after cloning:

```bash
# Step 1: Clone the repository
git clone https://github.com/ram07eng/airflow-watcher.git

# Step 2: Navigate to the project directory
cd airflow-watcher

# Step 3: Install the plugin (editable mode for development)
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

### Method 4: Add to requirements.txt

Add to your project's `requirements.txt`:

```txt
# From GitHub
airflow-watcher @ git+https://github.com/ram07eng/airflow-watcher.git

# Or with specific version
airflow-watcher @ git+https://github.com/ram07eng/airflow-watcher.git@v1.1.0
```

Then install:
```bash
pip install -r requirements.txt
```

Alternatively, use the bundled `requirements.txt` to install plugin dependencies directly:

```bash
pip install -r requirements.txt
```

This installs Apache Airflow, Flask, Requests, and Slack SDK — everything needed for the plugin.

### Method 5: Copy to plugins folder

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

### Method 6: Docker

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

## Plugin Configuration

The plugin can be configured via environment variables or `airflow.cfg`.

> **Environment variable conventions**
>
> Both the plugin and the standalone API read `AIRFLOW_WATCHER_*` (single underscore) env vars directly — you can use the same env var names for either component.
>
> The plugin also reads settings from `airflow.cfg` section `[airflow_watcher]` via Airflow's `conf.get()`. Airflow's native env override convention `AIRFLOW__AIRFLOW_WATCHER__<key>` works for that path too, but the simpler `AIRFLOW_WATCHER_*` vars are recommended and take precedence.
>
> | Method | Example | Works for |
> |--------|---------|-----------|
> | `AIRFLOW_WATCHER_*` env vars | `AIRFLOW_WATCHER_SLACK_WEBHOOK_URL=...` | Plugin + API |
> | `airflow.cfg [airflow_watcher]` | `slack_webhook_url = ...` | Plugin only |

### Environment Variables

Set these environment variables in your deployment:

```bash
# Slack Notifications
AIRFLOW_WATCHER_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz
AIRFLOW_WATCHER_SLACK_CHANNEL=#airflow-alerts

# Email Notifications
AIRFLOW_WATCHER_EMAIL_ENABLED=true
AIRFLOW_WATCHER_SMTP_HOST=smtp.example.com
AIRFLOW_WATCHER_SMTP_PORT=587
AIRFLOW_WATCHER_EMAIL_RECIPIENTS=team@example.com,oncall@example.com

# Monitoring Settings
AIRFLOW_WATCHER_FAILURE_LOOKBACK_HOURS=24
AIRFLOW_WATCHER_SLA_CHECK_INTERVAL_MINUTES=5
AIRFLOW_WATCHER_STALE_DAG_THRESHOLD_HOURS=24
```

### airflow.cfg

Alternatively, add an `[airflow_watcher]` section to your `airflow.cfg`:

```ini
[airflow_watcher]
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

## Plugin Verification

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

# Standalone API Installation

The standalone API runs as a **separate FastAPI process** outside the Airflow webserver. It connects directly to the Airflow metadata database and exposes REST endpoints at `/api/v1/*`.

### Install with standalone extras

```bash
# From PyPI
pip install airflow-watcher[standalone]

# Or from GitHub
pip install "airflow-watcher[standalone] @ git+https://github.com/ram07eng/airflow-watcher.git"

# Or in editable mode (development)
pip install -e ".[standalone]"
```

### Install from requirements file

Use the bundled `requirements-api.txt` — it includes all plugin dependencies plus FastAPI, Uvicorn, SQLAlchemy, python-dotenv, and psycopg2:

```bash
pip install -r requirements-api.txt
```

This is the recommended approach when you are not installing airflow-watcher as a package (e.g., deploying from a cloned repo).

This installs FastAPI, Uvicorn, and other dependencies needed for the standalone API.

---

## API Quick Start

```bash
# 1. Create .env file (already gitignored)
cat > .env << 'EOF'
AIRFLOW_WATCHER_DB_URI=postgresql://airflow:airflow@localhost:5432/airflow
AIRFLOW_WATCHER_API_KEYS=your-secret-api-key-here
EOF

# 2. Start the server
python src/airflow_watcher/api/main.py
# → Uvicorn running on http://0.0.0.0:8081

# 3. Test it
curl -H "Authorization: Bearer your-secret-api-key-here" \
     http://localhost:8081/api/v1/health/
```

Interactive API docs (Swagger UI) are available at **http://localhost:8081/docs** when running locally.

---

## API Authentication

All `/api/v1/*` endpoints require a Bearer token when `AIRFLOW_WATCHER_API_KEYS` is set:

```bash
curl -H "Authorization: Bearer <key>" http://localhost:8081/api/v1/failures/
```

| Scenario | Behavior |
|----------|----------|
| `API_KEYS` not set | Auth disabled — all requests pass through (dev mode) |
| `API_KEYS` set | Every request needs `Authorization: Bearer <key>` header |
| Invalid/missing token | `401 Unauthorized` |
| Multiple keys | Comma-separated — rotate independently per consumer |

**Security**: Tokens are compared using `secrets.compare_digest()` (constant-time) to prevent timing attacks.

---

## API Configuration Reference

Set these as environment variables (or in a `.env` file):

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AIRFLOW_WATCHER_DB_URI` | **Yes** | — | Airflow metadata DB connection string |
| `AIRFLOW_WATCHER_DB_READ_URI` | No | — | Read-replica DB connection string (see [Read-Replica Configuration](#read-replica-configuration)) |
| `AIRFLOW_WATCHER_API_KEYS` | No | _(disabled)_ | Comma-separated API keys for auth |
| `AIRFLOW_WATCHER_API_HOST` | No | `0.0.0.0` | Bind host |
| `AIRFLOW_WATCHER_API_PORT` | No | `8081` | Bind port |
| `AIRFLOW_WATCHER_CACHE_TTL` | No | `60` | Cache TTL in seconds |
| `AIRFLOW_WATCHER_QUERY_TIMEOUT_MS` | No | `30000` | PostgreSQL query timeout (ms) |
| `AIRFLOW_WATCHER_REQUEST_TIMEOUT_SECONDS` | No | `60` | HTTP request timeout (seconds) — increase for large Airflow installations |
| `AIRFLOW_WATCHER_DB_POOL_SIZE` | No | `5` | Persistent DB connections per process |
| `AIRFLOW_WATCHER_DB_MAX_OVERFLOW` | No | `10` | Max temporary connections above pool_size |
| `AIRFLOW_WATCHER_RATE_LIMIT_RPM` | No | `120` | Rate limit per IP (requests/min) |
| `AIRFLOW_WATCHER_LOG_FORMAT` | No | `text` | Log format: `text` or `json` (structured) |
| `AIRFLOW_WATCHER_LOG_LEVEL` | No | `INFO` | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `AIRFLOW_WATCHER_RBAC_ENABLED` | No | `false` | Enable per-key DAG filtering |
| `AIRFLOW_WATCHER_RBAC_KEY_DAG_MAPPING` | No | `{}` | JSON mapping: `{"key": ["dag1","dag2"]}` |
| `AIRFLOW_WATCHER_RBAC_FAIL_OPEN` | No | `true` | Grant full access on RBAC errors |
| `AIRFLOW_WATCHER_SLACK_WEBHOOK_URL` | No | — | Slack alert webhook |
| `AIRFLOW_WATCHER_PAGERDUTY_ROUTING_KEY` | No | — | PagerDuty routing key |
| `AIRFLOW_WATCHER_PROMETHEUS_ENABLED` | No | `false` | Enable `/metrics` endpoint |
| `AIRFLOW_WATCHER_STATSD_ENABLED` | No | `false` | Enable StatsD emission |

### Rate Limiting

The standalone API includes per-IP sliding-window rate limiting. When a client exceeds the limit, responses return **HTTP 429 Too Many Requests** with a `Retry-After` header.

| Setting | Default | Behavior |
|---------|---------|----------|
| `AIRFLOW_WATCHER_RATE_LIMIT_RPM` | `120` | Max requests per IP per 60-second window |

- `/healthz` is **exempt** from rate limiting (safe for high-frequency health probes).
- Each Gunicorn/Uvicorn **worker maintains its own counter**. With 4 workers, the effective limit per IP is up to `4 × 120 = 480 req/min`. For accurate cross-process limiting, place a shared rate limiter (e.g. Nginx `limit_req`, or Redis-backed middleware) in front of the API.
- Stale IP entries are automatically pruned every 5 minutes to prevent memory growth.

```bash
# Allow 60 requests/min per IP (stricter)
export AIRFLOW_WATCHER_RATE_LIMIT_RPM=60

# Allow 300 requests/min per IP (more relaxed)
export AIRFLOW_WATCHER_RATE_LIMIT_RPM=300
```

---

## API RBAC (Per-Key DAG Filtering)

When `AIRFLOW_WATCHER_RBAC_ENABLED=true`, each API key can only see its mapped DAGs:

```bash
export AIRFLOW_WATCHER_RBAC_ENABLED=true
export AIRFLOW_WATCHER_RBAC_KEY_DAG_MAPPING='{"team-a-key": ["weather_pipeline", "stock_collector"], "team-b-key": ["ecommerce_etl"], "admin-key": ["*"]}'
export AIRFLOW_WATCHER_API_KEYS="team-a-key,team-b-key,admin-key"
```

| Key | Sees |
|-----|------|
| `team-a-key` | Only `weather_pipeline` and `stock_collector` |
| `team-b-key` | Only `ecommerce_etl` |
| `admin-key` | All DAGs (`"*"` = full access) |

To grant full access, use `"*"` in the mapping, omit the key from the mapping, or disable RBAC.

---

## Connecting to the Metadata Database

Airflow Watcher reads directly from the **Airflow metadata database**. The standalone
API needs the same connection string your Airflow scheduler/webserver uses.

### Finding Your Connection String

```bash
# From any machine that has Airflow configured:
airflow config get-value core sql_alchemy_conn
```

Or inspect `airflow.cfg`:

```ini
[core]
sql_alchemy_conn = postgresql+psycopg2://airflow:password@db-host:5432/airflow
```

### Connection String Formats

| Backend | Format |
|---------|--------|
| **PostgreSQL** (recommended) | `postgresql+psycopg2://user:pass@host:5432/airflow` |
| **MySQL** | `mysql+mysqldb://user:pass@host:3306/airflow` |
| **SQLite** (dev only) | `sqlite:////path/to/airflow.db` |

### Cloud-Managed Database URIs

**AWS RDS (MWAA)**:
```bash
export AIRFLOW_WATCHER_DB_URI="postgresql+psycopg2://airflow:${DB_PASSWORD}@my-mwaa-cluster.abc123.us-east-1.rds.amazonaws.com:5432/airflow"
```

**Google Cloud SQL (Composer)**:
```bash
# Via Cloud SQL Auth Proxy (recommended):
export AIRFLOW_WATCHER_DB_URI="postgresql+psycopg2://airflow:${DB_PASSWORD}@127.0.0.1:5432/airflow"
# Start the proxy: cloud-sql-proxy my-project:us-central1:my-instance --port 5432
```

**Azure Database for PostgreSQL (Astronomer / AKS)**:
```bash
export AIRFLOW_WATCHER_DB_URI="postgresql+psycopg2://airflow@my-server:${DB_PASSWORD}@my-server.postgres.database.azure.com:5432/airflow?sslmode=require"
```

### SSL/TLS Connections

For databases requiring SSL (most cloud providers in production):

```bash
# PostgreSQL with SSL
export AIRFLOW_WATCHER_DB_URI="postgresql+psycopg2://user:pass@host:5432/airflow?sslmode=require"

# With a custom CA certificate
export AIRFLOW_WATCHER_DB_URI="postgresql+psycopg2://user:pass@host:5432/airflow?sslmode=verify-full&sslrootcert=/path/to/ca.pem"

# MySQL with SSL
export AIRFLOW_WATCHER_DB_URI="mysql+mysqldb://user:pass@host:3306/airflow?ssl_ca=/path/to/ca.pem"
```

### Read-Replica Configuration

For high-traffic deployments, point Airflow Watcher at a **read replica** to
avoid adding query load to the primary metadata database. All monitor queries
are read-only, making this safe and recommended for production.

```bash
# Primary (used for /healthz connectivity check)
export AIRFLOW_WATCHER_DB_URI="postgresql+psycopg2://airflow:pass@primary-db:5432/airflow"

# Read replica (all monitor queries route here)
export AIRFLOW_WATCHER_DB_READ_URI="postgresql+psycopg2://airflow:pass@replica-db:5432/airflow"
```

When `AIRFLOW_WATCHER_DB_READ_URI` is set:

- All monitor queries (`@provide_session`) run against the read replica.
- The primary engine is still used for `/healthz` connectivity checks.
- The `/healthz` response includes a `read_db_connected` field.
- Connection pooling settings (`DB_POOL_SIZE`, `DB_MAX_OVERFLOW`) apply to both engines.

> **Tip:** If you only have a single database, omit `AIRFLOW_WATCHER_DB_READ_URI`.
> The primary engine handles everything.

### Connection Pooling

Tune pool settings for your workload:

```bash
export AIRFLOW_WATCHER_DB_POOL_SIZE=5      # Persistent connections per process
export AIRFLOW_WATCHER_DB_MAX_OVERFLOW=10   # Burst connections above pool_size
export AIRFLOW_WATCHER_QUERY_TIMEOUT_MS=30000  # Per-query timeout (PostgreSQL)
```

**Guidelines:**
- 1–2 Gunicorn workers: `POOL_SIZE=5`, `MAX_OVERFLOW=10` (default)
- 4+ workers: reduce to `POOL_SIZE=3`, `MAX_OVERFLOW=5` to stay within DB limits
- Total max connections = `workers × (POOL_SIZE + MAX_OVERFLOW)`

---

## API Verification

1. **Liveness probe** (no auth required):
   ```bash
   curl http://localhost:8081/healthz
   # → {"status": "ok", "uptime_seconds": ..., "db_connected": true}
   # With read replica: {"status": "ok", ..., "db_connected": true, "read_db_connected": true}
   ```

2. **Health endpoint** (with auth):
   ```bash
   curl -H "Authorization: Bearer <key>" http://localhost:8081/api/v1/health/
   ```

3. **Swagger UI**: Open http://localhost:8081/docs in a browser.

---

# Infrastructure Integration

This section covers installing Airflow Watcher on production Airflow platforms. Pick the section that matches your infrastructure.

## AWS MWAA

### Setup

1. Add `airflow-watcher` to your MWAA `requirements.txt`:

```
airflow-watcher==1.1.0
```

For Prometheus metrics support:
```
airflow-watcher[all]==1.1.0
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
echo "airflow-watcher==1.1.0" >> requirements/requirements.txt
./mwaa-local-env build-image
./mwaa-local-env start
```

Visit `http://localhost:8080/api/watcher/health` to verify.

> **Note:** If using Slack or PagerDuty notifications, ensure your MWAA VPC has a NAT gateway for outbound internet access.

---

## Google Cloud Composer

### Setup

1. Install the plugin via Cloud Composer's PyPI packages:

```bash
gcloud composer environments update <your-environment> \
  --location <region> \
  --update-pypi-package airflow-watcher==1.1.0
```

For Prometheus metrics support:

```bash
gcloud composer environments update <your-environment> \
  --location <region> \
  --update-pypi-package "airflow-watcher[all]==1.1.0"
```

2. Set environment variables (optional):

```bash
gcloud composer environments update <your-environment> \
  --location <region> \
  --update-env-variables \
    AIRFLOW_WATCHER_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz,\
    AIRFLOW_WATCHER_PAGERDUTY_ROUTING_KEY=your-key,\
    AIRFLOW_WATCHER_PROMETHEUS_ENABLED=true
```

3. Override Airflow configuration (alternative to env vars):

```bash
gcloud composer environments update <your-environment> \
  --location <region> \
  --update-airflow-configs \
    watcher-slack_webhook_url=https://hooks.slack.com/services/xxx/yyy/zzz,\
    watcher-slack_channel=#airflow-alerts
```

4. Verify the plugin is loaded:

```
https://<your-composer-url>/api/watcher/health
```

### Networking

- **Private IP Composer**: Ensure the VPC has a Cloud NAT or proxy for outbound access to Slack/PagerDuty webhooks.
- **Shared VPC**: Firewall rules must allow egress on ports 443 (Slack, PagerDuty) and 8125 (StatsD, if applicable).

### Composer 2 vs Composer 1

| Feature | Composer 2 | Composer 1 |
|---------|-----------|-----------|
| Plugin auto-discovery | Yes (Airflow 2.7+ entry point) | Copy to `plugins/` folder in GCS bucket |
| PyPI install | `gcloud ... --update-pypi-package` | Same |
| Private IP networking | Cloud NAT required | Cloud NAT required |

For Composer 1, if auto-discovery doesn't work, upload the plugin manually:

```bash
gsutil cp -r src/airflow_watcher gs://<composer-bucket>/plugins/airflow_watcher
```

---

## Kubernetes / Helm (Airflow Helm Chart)

### Plugin Installation via Helm Values

Add airflow-watcher to the official [Apache Airflow Helm chart](https://airflow.apache.org/docs/helm-chart/stable/index.html) configuration:

**values.yaml:**

```yaml
# Install the plugin as a pip package
airflowHome: /opt/airflow

# Option 1: Extra pip packages (recommended)
extraPipPackages:
  - "airflow-watcher==1.1.0"

# Option 2: Custom image (for faster startup)
# Build a custom image with the plugin pre-installed
# images:
#   airflow:
#     repository: your-registry/airflow-watcher
#     tag: "2.7.3-watcher-1.1.0"

# Environment variables for the webserver
env:
  - name: AIRFLOW_WATCHER_SLACK_WEBHOOK_URL
    valueFrom:
      secretKeyRef:
        name: airflow-watcher-secrets
        key: slack-webhook-url
  - name: AIRFLOW_WATCHER_PAGERDUTY_ROUTING_KEY
    valueFrom:
      secretKeyRef:
        name: airflow-watcher-secrets
        key: pagerduty-routing-key
  - name: AIRFLOW_WATCHER_PROMETHEUS_ENABLED
    value: "true"

# Prometheus ServiceMonitor (if using Prometheus Operator)
extraObjects:
  - apiVersion: monitoring.coreos.com/v1
    kind: ServiceMonitor
    metadata:
      name: airflow-watcher
      labels:
        release: prometheus
    spec:
      selector:
        matchLabels:
          component: webserver
      endpoints:
        - port: airflow-ui
          path: /watcher/metrics
          interval: 30s
```

### Secrets

Create a Kubernetes secret for sensitive values:

```bash
kubectl create secret generic airflow-watcher-secrets \
  --namespace airflow \
  --from-literal=slack-webhook-url='https://hooks.slack.com/services/xxx/yyy/zzz' \
  --from-literal=pagerduty-routing-key='your-key'
```

### Custom Docker Image (recommended for production)

```dockerfile
FROM apache/airflow:2.7.3-python3.10
RUN pip install --no-cache-dir airflow-watcher==1.1.0
```

```bash
docker build -t your-registry/airflow-watcher:2.7.3-1.1.0 .
docker push your-registry/airflow-watcher:2.7.3-1.1.0
```

Then reference it in `values.yaml`:

```yaml
images:
  airflow:
    repository: your-registry/airflow-watcher
    tag: "2.7.3-1.1.0"
```

### Deploying the Standalone API as a Sidecar or Separate Deployment

To run the standalone FastAPI service alongside Airflow on Kubernetes:

```yaml
# standalone-api-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-watcher-api
  namespace: airflow
spec:
  replicas: 2
  selector:
    matchLabels:
      app: airflow-watcher-api
  template:
    metadata:
      labels:
        app: airflow-watcher-api
    spec:
      containers:
        - name: watcher-api
          image: your-registry/airflow-watcher:2.7.3-1.1.0
          command: ["airflow-watcher-api"]
          ports:
            - containerPort: 8081
          env:
            - name: AIRFLOW_WATCHER_DB_URI
              valueFrom:
                secretKeyRef:
                  name: airflow-watcher-secrets
                  key: db-uri
            - name: AIRFLOW_WATCHER_API_KEYS
              valueFrom:
                secretKeyRef:
                  name: airflow-watcher-secrets
                  key: api-keys
            - name: AIRFLOW_WATCHER_PROMETHEUS_ENABLED
              value: "true"
          livenessProbe:
            httpGet:
              path: /healthz
              port: 8081
            initialDelaySeconds: 10
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /healthz
              port: 8081
            initialDelaySeconds: 5
            periodSeconds: 10
          resources:
            requests:
              cpu: 100m
              memory: 256Mi
            limits:
              cpu: 500m
              memory: 512Mi
---
apiVersion: v1
kind: Service
metadata:
  name: airflow-watcher-api
  namespace: airflow
spec:
  selector:
    app: airflow-watcher-api
  ports:
    - port: 8081
      targetPort: 8081
```

Apply:

```bash
kubectl apply -f standalone-api-deployment.yaml
```

---

## Production Deployment (Standalone API)

Do not run `python main.py` in production. Use one of the following approaches.

### Behind a Reverse Proxy (Nginx)

```nginx
upstream watcher_api {
    server 127.0.0.1:8081;
    server 127.0.0.1:8082;  # optional: multiple workers
}

server {
    listen 443 ssl;
    server_name watcher.yourcompany.com;

    ssl_certificate     /etc/ssl/certs/watcher.crt;
    ssl_certificate_key /etc/ssl/private/watcher.key;

    location / {
        proxy_pass http://watcher_api;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### Gunicorn with Uvicorn Workers

```bash
pip install gunicorn

gunicorn airflow_watcher.api.main:app \
  --workers 4 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8081 \
  --access-logfile - \
  --timeout 120
```

### systemd Service

```ini
# /etc/systemd/system/airflow-watcher-api.service
[Unit]
Description=Airflow Watcher Standalone API
After=network.target postgresql.service

[Service]
Type=simple
User=airflow
Group=airflow
WorkingDirectory=/opt/airflow-watcher
EnvironmentFile=/opt/airflow-watcher/.env
ExecStart=/opt/airflow-watcher/.venv/bin/gunicorn \
    airflow_watcher.api.main:app \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --bind 0.0.0.0:8081 \
    --access-logfile -
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable airflow-watcher-api
sudo systemctl start airflow-watcher-api
```

### Docker Compose (Production)

```yaml
services:
  watcher-api:
    image: your-registry/airflow-watcher:2.7.3-1.1.0
    command: >
      gunicorn airflow_watcher.api.main:app
      --workers 4
      --worker-class uvicorn.workers.UvicornWorker
      --bind 0.0.0.0:8081
      --access-logfile -
    ports:
      - "8081:8081"
    env_file:
      - .env
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/healthz"]
      interval: 30s
      timeout: 5s
      retries: 3
```

### Production Checklist

| Item | Details |
|------|---------|
| **TLS** | Terminate TLS at the reverse proxy or load balancer — never expose plain HTTP externally |
| **API keys** | Always set `AIRFLOW_WATCHER_API_KEYS` — do not run with auth disabled |
| **RBAC** | Enable `AIRFLOW_WATCHER_RBAC_ENABLED` and map keys to DAGs for multi-tenant setups |
| **Workers** | Run 2–4 Gunicorn workers per CPU core |
| **Connection pooling** | Each worker maintains its own pool. With 4 workers × `pool_size=5`, expect up to 20 persistent + 40 overflow connections. Tune `DB_POOL_SIZE` and `DB_MAX_OVERFLOW` to stay within your DB's `max_connections` |
| **Health checks** | Use `/healthz` for liveness probes — it checks DB connectivity |
| **Secrets** | Store `DB_URI` and `API_KEYS` in a secret manager (Vault, AWS Secrets Manager, K8s secrets) |
| **Logging** | Pipe access logs to your centralized logging platform |
| **Monitoring** | Enable Prometheus (`AIRFLOW_WATCHER_PROMETHEUS_ENABLED=true`) and scrape `/metrics` |

---

## Upgrading & Migration

### Upgrading from 0.1.x to 2.0.0

v2.0.0 adds the standalone API alongside the existing plugin. **The plugin is fully backwards compatible** — no config changes are needed if you only use the plugin.

| Change | Action |
|--------|--------|
| **Python 3.9 dropped** | Upgrade to Python 3.10+ before installing v2.0.0 |
| **Airflow pin** | Dependency changed to `>=2.7.0,<3.0`. If you run Airflow <2.7, stay on 0.1.x |
| **New extras** | The `[standalone]` extra is new; existing `pip install airflow-watcher` installs the plugin only — nothing breaks |

### Adding the Standalone API alongside an Existing Plugin

The plugin and standalone API **can coexist**. They share the same codebase but run in separate processes and use independent configurations.

```bash
# 1. Upgrade the package (plugin continues working)
pip install --upgrade airflow-watcher[standalone]

# 2. Configure the API to point at the same metadata DB
cat > /opt/airflow-watcher/.env << 'EOF'
AIRFLOW_WATCHER_DB_URI=postgresql://airflow:airflow@your-db:5432/airflow
AIRFLOW_WATCHER_API_KEYS=your-api-key
EOF

# 3. Start the API as a separate process
airflow-watcher-api
# or with Gunicorn:
gunicorn airflow_watcher.api.main:app --workers 2 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8081
```

**Key points:**
- Both components read `AIRFLOW_WATCHER_*` env vars. The plugin also reads `airflow.cfg [airflow_watcher]`.
- They share the same Airflow metadata database but maintain separate auth, caching, and rate-limiting.
- The plugin is accessible on the Airflow webserver port (default 8080); the API runs on its own port (default 8081).

### Upgrading within 2.x

```bash
pip install --upgrade airflow-watcher
# or
pip install --upgrade airflow-watcher[standalone]
```

Check [CHANGELOG.md](CHANGELOG.md) for breaking changes between releases.

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
   echo $AIRFLOW_WATCHER_SLACK_WEBHOOK_URL
   ```

### Standalone API: "DB_URI is required" error

The standalone API requires `AIRFLOW_WATCHER_DB_URI` to be set. Create a `.env` file or export the variable:

```bash
export AIRFLOW_WATCHER_DB_URI=postgresql://airflow:airflow@localhost:5432/airflow
```

### Standalone API: 401 Unauthorized

If `AIRFLOW_WATCHER_API_KEYS` is set, every request needs a Bearer token:

```bash
curl -H "Authorization: Bearer your-key" http://localhost:8081/api/v1/health/
```

To run without auth (dev mode), unset `AIRFLOW_WATCHER_API_KEYS`.

### Standalone API: Connection refused

Ensure the API is running and the port is correct:

```bash
# Check if process is running
lsof -i :8081

# Check bind address (default 0.0.0.0:8081)
echo $AIRFLOW_WATCHER_API_HOST $AIRFLOW_WATCHER_API_PORT
```

---

## Uninstallation

To remove the plugin and/or standalone API:

```bash
# If installed via pip (removes both plugin and API)
pip uninstall airflow-watcher

# If copied to plugins folder (plugin only)
rm -rf $AIRFLOW_HOME/plugins/airflow_watcher/
```

Then restart your Airflow webserver and stop any running standalone API processes.

---

## Support

- **Issues**: https://github.com/ram07eng/airflow-watcher/issues
- **Documentation**: https://github.com/ram07eng/airflow-watcher
