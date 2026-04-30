# Airflow Watcher 👁️

[![CI](https://github.com/ram07eng/airflow-watcher/actions/workflows/ci.yml/badge.svg)](https://github.com/ram07eng/airflow-watcher/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](https://www.python.org/downloads/)
[![Airflow 2.7–2.10](https://img.shields.io/badge/airflow-2.7–2.10-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000)](https://docs.astral.sh/ruff/)

An Airflow plugin and standalone REST API for monitoring DAG failures, SLA misses, task health, and scheduling delays — with built-in alerting via Slack, Email, and PagerDuty.

Airflow Watcher ships two independent components that share the same monitors, notifiers, and metrics layer:

| Component | Runs in | Auth | Use case |
|-----------|---------|------|----------|
| **[Plugin](#plugin-airflow-ui)** | Airflow webserver process | Airflow session cookies + FAB RBAC | UI dashboards, internal API at `/api/watcher` |
| **[Standalone API](#standalone-api-fastapi)** | Separate FastAPI process | Bearer token + per-key RBAC | External integrations, CI/CD, dashboards, automation |

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Alerting & Monitoring](#alerting--monitoring)
- **Plugin**
  - [Overview](#plugin-airflow-ui)
  - [Architecture](#plugin-architecture)
  - [UI Views](#ui-views)
  - [RBAC](#plugin-rbac)
  - [API Endpoints — `/api/watcher`](#plugin-api-endpoints--apiwatcher)
  - [Demo Environment](#demo-environment)
- **Standalone API**
  - [Overview](#standalone-api-fastapi)
  - [Architecture](#api-architecture)
  - [Quick Start](#quick-start)
  - [Authentication & Configuration](#authentication--configuration)
  - [API Endpoints — `/api/v1`](#api-endpoints)
  - [RBAC, Request Flow & Integration Examples](#api-rbac-request-flow--integration-examples)
- **Common**
  - [Plugin vs API Comparison](#plugin-vs-api-comparison)
  - [Project Structure](#project-structure)
  - [Testing](#testing)
  - [Infrastructure Integration](#infrastructure-integration)
  - [Development](#development)

---

## Features

### Plugin Features
- 📊 **Dashboard View** — 7 custom Airflow UI pages for monitoring
- 🔒 **Airflow RBAC** — DAG-level filtering via Airflow's FAB security manager
- 🔌 **Zero Config** — Auto-registers via Airflow's plugin entry point
- 📡 **Internal REST API** — `/api/watcher/*` endpoints on the webserver

### API Features
- 🚀 **Standalone FastAPI** — Runs outside the Airflow webserver
- 🔑 **Bearer Token Auth** — Constant-time token validation with key rotation
- 🗂️ **Per-Key RBAC** — Map API keys to specific DAGs
- 📄 **Swagger UI** — Interactive docs at `/docs`
- ⚡ **Response Caching** — Thread-safe TTL cache (default 60s)
- 🚦 **Rate Limiting** — Per-IP sliding window (default 120 req/min, returns 429 with `Retry-After`)
- 📋 **Structured Logging** — JSON log format with request IDs (`AIRFLOW_WATCHER_LOG_FORMAT=json`)
- `/healthz` **Liveness Probe** — No auth required, checks DB connectivity

### Shared Features
- 🚨 **DAG Failure Monitoring** — Real-time tracking of DAG and task failures
- ⏰ **SLA Miss Detection** — Alerts when DAGs miss their SLA deadlines
- 📈 **Trend Analysis** — Historical failure and SLA miss trends
- 🔔 **Multi-channel Notifications** — Slack, Email, and PagerDuty alerts
- 📡 **Metrics Export** — StatsD/Datadog and Prometheus support
- ⚙️ **Flexible Alert Rules** — Pre-defined templates or custom rules

## Airflow Version Compatibility

| Airflow Version | Status |
|-----------------|--------|
| 2.7.x – 2.10.x | **Fully supported** — tested in CI |
| 2.5.x – 2.6.x | Should work, not actively tested |
| **3.x** | **Not yet supported** — the `<3.0` pin in `pyproject.toml` is intentional. Airflow 3.0 removes `airflow.models.SlaMiss` (used by `SLAMonitor`) and changes the metadata DB schema. A compatibility release is planned; track progress in the [issue tracker](https://github.com/ram07eng/airflow-watcher/issues). |

## Installation

📖 **See [INSTALL.md](INSTALL.md) for detailed installation and configuration instructions.**

## Alerting & Monitoring

📖 **See [ALERTING.md](ALERTING.md) for complete alerting configuration:**

- **Slack** — Rich notifications with blocks
- **Email** — SMTP-based alerts
- **PagerDuty** — Incident management with deduplication
- **StatsD/Datadog** — Real-time metrics
- **Prometheus** — `/metrics` endpoint for scraping

### Quick Alerting Setup

```bash
# Slack alerts
export AIRFLOW_WATCHER_SLACK_WEBHOOK_URL="https://hooks.slack.com/..."

# PagerDuty (optional)
export AIRFLOW_WATCHER_PAGERDUTY_ROUTING_KEY="your-key"

# Choose alert template
export AIRFLOW_WATCHER_ALERT_TEMPLATE="production_balanced"
```

---

# Plugin (Airflow UI)

The plugin runs **inside** the Airflow webserver process. Once installed, it auto-registers with Airflow, adds a "Watcher" menu to the UI, and exposes REST endpoints at `/api/watcher/*`. No separate workers, message queues, or external databases are needed — it reads from the same metadata DB that Airflow already maintains.

## Demo

![Airflow Watcher Demo](https://raw.githubusercontent.com/ram07eng/airflow-watcher/main/docs/images/demo_new.gif)

<details>
<summary><h2>Plugin Architecture</h2></summary>

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

</details>

## UI Views

Once installed, navigate to **Watcher** in the Airflow UI navigation to access:

| Menu Item | Description |
|-----------|-------------|
| **Airflow Dashboard** | Overview metrics |
| **Airflow Health** | DAG health status (success/failed/delayed/stale) |
| **DAG Scheduling** | Queue and pool utilization |
| **DAG Failures** | Recent failures with details |
| **SLA Tracker** | SLA misses and delays |
| **Task Health** | Long-running and zombie tasks |
| **Dependencies** | Cross-DAG dependency tracking |

<details>
<summary><h2>Plugin RBAC</h2></summary>

The plugin integrates with Airflow's built-in FAB security manager to enforce DAG-level access control. No separate configuration is needed — it reads directly from Airflow's role and permission system.

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
| All `/api/watcher` endpoints | Same RBAC enforcement as UI pages |

### RBAC Demo

**Admin** — sees all DAGs:

![Admin RBAC Demo](https://raw.githubusercontent.com/ram07eng/airflow-watcher/main/docs/images/rbac_admin.gif)

**Weather user** — sees only weather & stock DAGs:

![Weather User RBAC Demo](https://raw.githubusercontent.com/ram07eng/airflow-watcher/main/docs/images/rbac_weather.gif)

**Ecommerce user** — sees only ecommerce & data quality DAGs:

![Ecommerce User RBAC Demo](https://raw.githubusercontent.com/ram07eng/airflow-watcher/main/docs/images/rbac_ecommerce.gif)

</details>

<details>
<summary><h2>Plugin API Endpoints — <code>/api/watcher</code></h2></summary>

The plugin exposes a REST API at `/api/watcher/*` on the Airflow webserver. Authentication uses Airflow's session cookies (same login as the UI). All endpoints return JSON with a standard `{status, data, timestamp}` envelope.

#### Failures

| Method | Path | Params | Description |
|--------|------|--------|-------------|
| GET | `/api/watcher/failures` | `dag_id`, `hours`, `limit` | Recent DAG failures |
| GET | `/api/watcher/failures/stats` | `hours` | Failure rate statistics |

#### SLA

| Method | Path | Params | Description |
|--------|------|--------|-------------|
| GET | `/api/watcher/sla/misses` | `dag_id`, `hours`, `limit` | SLA miss events |
| GET | `/api/watcher/sla/stats` | `hours` | SLA miss statistics |

#### Health

| Method | Path | Params | Description |
|--------|------|--------|-------------|
| GET | `/api/watcher/health` | — | System health summary (200 healthy, 503 degraded) |
| GET | `/api/watcher/health/<dag_id>` | — | Health for a specific DAG |

#### Tasks

| Method | Path | Params | Description |
|--------|------|--------|-------------|
| GET | `/api/watcher/tasks/long-running` | `threshold_minutes` | Tasks exceeding duration threshold |
| GET | `/api/watcher/tasks/retries` | `hours`, `min_retries` | Tasks with excessive retries |
| GET | `/api/watcher/tasks/zombies` | `threshold_minutes` | Potential zombie tasks |
| GET | `/api/watcher/tasks/failure-patterns` | `hours` | Task failure pattern analysis |

#### Scheduling

| Method | Path | Params | Description |
|--------|------|--------|-------------|
| GET | `/api/watcher/scheduling/lag` | `hours`, `threshold_minutes` | Scheduling delay percentiles |
| GET | `/api/watcher/scheduling/queue` | — | Current queue status |
| GET | `/api/watcher/scheduling/pools` | — | Pool utilization |
| GET | `/api/watcher/scheduling/stale-dags` | `expected_interval_hours` | DAGs not running on schedule |
| GET | `/api/watcher/scheduling/concurrent` | — | DAGs with multiple concurrent runs |

#### DAGs

| Method | Path | Params | Description |
|--------|------|--------|-------------|
| GET | `/api/watcher/dags/import-errors` | — | DAG import errors |
| GET | `/api/watcher/dags/status-summary` | — | DAG status summary with health score |
| GET | `/api/watcher/dags/complexity` | — | DAG complexity analysis |
| GET | `/api/watcher/dags/inactive` | `days` | Inactive DAGs |

#### Dependencies

| Method | Path | Params | Description |
|--------|------|--------|-------------|
| GET | `/api/watcher/dependencies/upstream-failures` | `hours` | Upstream failure cascade analysis |
| GET | `/api/watcher/dependencies/cross-dag` | — | Cross-DAG dependencies |
| GET | `/api/watcher/dependencies/correlations` | `hours` | Failure correlations |
| GET | `/api/watcher/dependencies/impact/<dag_id>/<task_id>` | — | Downstream impact analysis |

#### Overview

| Method | Path | Params | Description |
|--------|------|--------|-------------|
| GET | `/api/watcher/overview` | — | Combined overview of all monitoring data |

</details>

<details>
<summary><h2>Demo Environment</h2></summary>

To test the plugin locally with sample DAGs and pre-configured RBAC users:

```bash
cd demo
docker-compose up -d
```

Then visit http://localhost:8080 and navigate to the **Watcher** menu.

| User | Password | Role | Visible DAGs |
|------|----------|------|-------------|
| `admin` | `admin` | Admin | All 8 DAGs |
| `weather_user` | `weather123` | team_weather | weather_data_pipeline, stock_market_collector |
| `ecommerce_user` | `ecommerce123` | team_ecommerce | ecommerce_sales_etl, data_quality_checks |

See [demo/README.md](demo/README.md) for more details.

</details>

---

# Standalone API (FastAPI)

A lightweight, standalone REST API that runs **outside** the Airflow webserver. Use this when you want to call monitoring endpoints from external services, dashboards, or CI/CD pipelines without adding load to the Airflow webserver.

📖 **[Interactive API Docs (Swagger UI)](https://ram07eng.github.io/airflow-watcher/)** — browse all 28 endpoints without running the server.

<details>
<summary><h2>API Architecture</h2></summary>

```
                                 ┌─────────────────┐
                                 │  External Client │
                                 │  (curl / app)    │
                                 └────────┬─────────┘
                                          │
                              Authorization: Bearer <key>
                                          │
                                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Standalone FastAPI Service                   │
│                    http://localhost:8081                        │
│                                                                 │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌────────────┐    │
│  │  Auth    │──▶│  RBAC    │──▶│  Cache   │──▶│  Monitors  │    │
│  │(Bearer)  │   │(dag map) │   │ (TTL 60s)│   │  (6 core)  │    │
│  └──────────┘   └──────────┘   └──────────┘   └──────┬─────┘    │
│                                                       │         │
│  ┌──────────────────────────────────────────────────┐ │         │
│  │              API Routers (11)                     │ │        │
│  │  /failures /sla /tasks /scheduling /dags          │◀┘        │
│  │  /dependencies /overview /health /alerts          │          │
│  │  /cache /metrics                                  │          │
│  └──────────────────────────────────────────────────┘           │
│                         │                                       │
│  ┌──────────────┐  ┌────▼──────┐  ┌─────────────────────────┐   │
│  │  Notifiers   │  │  Envelope │  │  Emitters               │   │
│  │  Slack/Email │  │  {status, │  │  StatsD / Prometheus    │   │
│  │  PagerDuty   │  │   data}   │  │  /metrics               │   │
│  └──────────────┘  └───────────┘  └─────────────────────────┘   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
               ┌───────────────────────┐
               │  Airflow Metadata DB  │
               │  (PostgreSQL / MySQL) │
               └───────────────────────┘
```

</details>

## Quick Start

📖 **See [INSTALL.md — API Quick Start](INSTALL.md#api-quick-start) for setup instructions.**

```bash
# 1. Install standalone extras
pip install -e ".[standalone]"

# 2. Create .env file (already gitignored)
cat > .env << 'EOF'
AIRFLOW_WATCHER_DB_URI=postgresql://airflow:airflow@localhost:5432/airflow
AIRFLOW_WATCHER_API_KEYS=your-secret-api-key-here
EOF

# 3. Start the server
python src/airflow_watcher/api/main.py
# → Uvicorn running on http://0.0.0.0:8081

# 4. Test it
curl -H "Authorization: Bearer your-secret-api-key-here" \
     http://localhost:8081/api/v1/health/
```

### Interactive API Docs

📖 **Browse the full API spec online: [Airflow Watcher API Docs](https://ram07eng.github.io/airflow-watcher/)** (Swagger UI — no server required)

When running locally, the same docs are available at **http://localhost:8081/docs**.

## Authentication & Configuration

📖 **See [INSTALL.md](INSTALL.md#api-authentication) for full authentication, configuration, and RBAC setup.**

<details>
<summary><h2>API Endpoints</h2></summary>

All endpoints return a standard JSON envelope:

```json
{
  "status": "success",
  "data": { ... },
  "timestamp": "2026-03-27T12:34:56.789000Z"
}
```

#### Failures — `/api/v1/failures`

| Method | Path | Params | Description |
|--------|------|--------|-------------|
| GET | `/` | `dag_id`, `hours` (1–8760, default 24), `limit` (1–500, default 50), `offset` (default 0) | Recent DAG failures |
| GET | `/stats` | `hours` (1–8760, default 24) | Failure rate statistics |

#### SLA — `/api/v1/sla`

| Method | Path | Params | Description |
|--------|------|--------|-------------|
| GET | `/misses` | `dag_id`, `hours` (1–8760, default 24), `limit` (1–500, default 50), `offset` (default 0) | SLA miss events |
| GET | `/stats` | `hours` (1–8760, default 24) | SLA miss statistics |

#### Tasks — `/api/v1/tasks`

| Method | Path | Params | Description |
|--------|------|--------|-------------|
| GET | `/long-running` | `threshold_minutes` (1–10080, default 60) | Tasks exceeding duration threshold |
| GET | `/retries` | `hours` (1–8760, default 24), `min_retries` (1–100, default 2) | Tasks with excessive retries |
| GET | `/zombies` | `threshold_minutes` (1–10080, default 120) | Potential zombie tasks |
| GET | `/failure-patterns` | `hours` (1–8760, default 168) | Task failure pattern analysis |

#### Scheduling — `/api/v1/scheduling`

| Method | Path | Params | Description |
|--------|------|--------|-------------|
| GET | `/lag` | `hours` (1–8760, default 24), `threshold_minutes` (1–10080, default 10) | Scheduling delay percentiles |
| GET | `/queue` | — | Current queue status |
| GET | `/pools` | — | Pool utilization |
| GET | `/stale-dags` | `expected_interval_hours` (1–720, default 24) | DAGs not running on schedule |
| GET | `/concurrent` | — | DAGs with multiple concurrent runs |

#### DAGs — `/api/v1/dags`

| Method | Path | Params | Description |
|--------|------|--------|-------------|
| GET | `/import-errors` | — | DAG import/parse errors |
| GET | `/status-summary` | — | Overall DAG counts and health score |
| GET | `/complexity` | — | DAGs ranked by task count |
| GET | `/inactive` | `days` (1–365, default 30) | Active DAGs with no recent runs |

#### Dependencies — `/api/v1/dependencies`

| Method | Path | Params | Description |
|--------|------|--------|-------------|
| GET | `/upstream-failures` | `hours` (1–8760, default 24) | Tasks in upstream_failed state |
| GET | `/cross-dag` | — | Cross-DAG dependency map |
| GET | `/correlations` | `hours` (1–8760, default 24) | Failure correlations between DAGs |
| GET | `/impact/{dag_id}/{task_id}` | — | Downstream cascading failure impact |

#### Overview — `/api/v1/overview`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Combined snapshot of all monitors (cached 60s) |

#### Health — `/api/v1/health`

| Method | Path | HTTP Code | Description |
|--------|------|-----------|-------------|
| GET | `/` | 200 if healthy, 503 if degraded | System health (score ≥ 70 & no import errors = healthy) |
| GET | `/{dag_id}` | 200 | Per-DAG health with recent failures and SLA misses |

#### Alerts — `/api/v1/alerts`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/rules` | List all configured alert rules |
| POST | `/evaluate` | Evaluate rules and dispatch notifications |

#### Cache — `/api/v1/cache`

| Method | Path | Description |
|--------|------|-------------|
| POST | `/invalidate` | Clear all cached metrics |

#### Metrics — `/metrics` _(optional, root level)_

| Method | Path | Description |
|--------|------|-------------|
| GET | `/metrics` | Prometheus exposition format (requires `AIRFLOW_WATCHER_PROMETHEUS_ENABLED=true`) |

#### Infrastructure — `/healthz` _(no auth required)_

| Method | Path | Description |
|--------|------|-------------|
| GET | `/healthz` | Liveness probe: `{status: "ok"/"degraded", uptime_seconds, db_connected}` |

<details>
<summary><strong>Example API Responses</strong> (click to expand)</summary>

**GET /api/v1/overview/**
```json
{
  "status": "success",
  "data": {
    "failure_stats": {"total_runs": 142, "failed_runs": 3, "failure_rate": 2.11, "unique_failed_dags": 2,
      "most_failing_dags": [{"dag_id": "etl_customers", "failure_count": 2}]},
    "sla_stats": {"total_misses": 1, "top_dags_with_misses": [{"dag_id": "etl_customers", "miss_count": 1}]},
    "long_running_tasks": 0, "zombie_count": 0,
    "queue_status": {"queued_count": 2, "scheduled_count": 5},
    "dag_summary": {"total_dags": 18, "active_dags": 15, "paused_dags": 3, "health_score": 92},
    "import_errors": 0
  },
  "timestamp": "2026-04-10T08:15:32.456000Z"
}
```

**GET /api/v1/health/** — returns HTTP 200 when healthy, 503 when degraded
```json
{
  "status": "success",
  "data": {
    "status": "healthy", "health_score": 92,
    "summary": {"total_dags": 18, "active_dags": 15, "paused_dags": 3, "health_score": 92},
    "dag_health": {"healthy": 15, "degraded": 2, "critical": 1},
    "import_error_count": 0
  },
  "timestamp": "2026-04-10T08:15:33.012000Z"
}
```

**GET /api/v1/failures/?hours=24**
```json
{
  "status": "success",
  "data": {
    "failures": [{"dag_id": "etl_customers", "run_id": "scheduled__2026-04-10T06:00:00+00:00",
      "state": "failed", "duration": 153.0,
      "failed_tasks": [{"task_id": "load_to_bq", "operator": "BigQueryInsertJobOperator",
        "try_number": 3, "max_tries": 3, "state": "failed"}]}],
    "count": 1, "filters": {"dag_id": null, "hours": 24}
  },
  "timestamp": "2026-04-10T08:15:34.789000Z"
}
```

**GET /healthz**
```json
{"status": "ok", "uptime_seconds": 3612.45, "db_connected": true, "read_db_connected": true}
```
`read_db_connected` only appears when a read-replica is configured.

</details>

</details>

<details>
<summary><h2>API RBAC, Request Flow & Integration Examples</h2></summary>

### API RBAC (Role-Based Access Control)

When `AIRFLOW_WATCHER_RBAC_ENABLED=true`, each API key can only see its mapped DAGs:

```bash
export AIRFLOW_WATCHER_RBAC_ENABLED=true
export AIRFLOW_WATCHER_RBAC_KEY_DAG_MAPPING='{"team-a-key": ["weather_pipeline", "stock_collector"], "team-b-key": ["ecommerce_etl"]}'
export AIRFLOW_WATCHER_API_KEYS="team-a-key,team-b-key,admin-key"
```

| Key | Sees |
|-----|------|
| `team-a-key` | Only `weather_pipeline` and `stock_collector` |
| `team-b-key` | Only `ecommerce_etl` |
| `admin-key` | Empty results (not in mapping — denied by default) |

To grant full access to a key, add it to the mapping with `["*"]`:

```bash
export AIRFLOW_WATCHER_RBAC_KEY_DAG_MAPPING='{"team-a-key": ["weather_pipeline"], "admin-key": ["*"]}'
```

Or set `AIRFLOW_WATCHER_RBAC_FAIL_OPEN=true` to allow unmapped keys to see all DAGs (not recommended for production).

### Request Flow

```
Client Request
  │
  ├─▶ Middleware: adds X-API-Version: 1.0 header
  │
  ├─▶ Auth Dependency: validates Bearer token (constant-time)
  │     └── 401 if invalid/missing
  │
  ├─▶ RBAC Dependency: resolves allowed DAG IDs for this key
  │     └── 403 if DAG not in allowed set
  │
  ├─▶ Cache Layer: check MetricsCache (thread-safe, TTL-based)
  │     ├── HIT  → return cached response
  │     └── MISS → call monitor
  │
  ├─▶ Monitor: queries Airflow metadata DB via SQLAlchemy
  │
  ├─▶ RBAC Filter: strips DAGs the caller isn't allowed to see
  │
  └─▶ Envelope: wraps in {status, data, timestamp} → HTTP 200
```

### Integration Examples

**curl:**
```bash
# Health check (no auth)
curl http://localhost:8081/healthz

# Get failures (with auth)
curl -H "Authorization: Bearer $API_KEY" \
     "http://localhost:8081/api/v1/failures/?hours=24&limit=10"

# Get overview snapshot
curl -H "Authorization: Bearer $API_KEY" \
     http://localhost:8081/api/v1/overview/

# Evaluate and fire alerts
curl -X POST -H "Authorization: Bearer $API_KEY" \
     http://localhost:8081/api/v1/alerts/evaluate

# Clear cache
curl -X POST -H "Authorization: Bearer $API_KEY" \
     http://localhost:8081/api/v1/cache/invalidate
```

**Python:**
```python
import requests

API_URL = "http://localhost:8081"
HEADERS = {"Authorization": "Bearer your-secret-key"}

# Get system health
resp = requests.get(f"{API_URL}/api/v1/health/", headers=HEADERS)
health = resp.json()["data"]
print(f"Score: {health['health_score']}, Status: {health['status']}")

# Get recent failures
resp = requests.get(f"{API_URL}/api/v1/failures/", headers=HEADERS,
                    params={"hours": 12, "limit": 20})
for failure in resp.json()["data"]["failures"]:
    print(f"  {failure['dag_id']} failed at {failure['execution_date']}")

# Get scheduling lag
resp = requests.get(f"{API_URL}/api/v1/scheduling/lag", headers=HEADERS)
lag = resp.json()["data"]
print(f"p95 lag: {lag['scheduling_lag']['p95']}s")
```

**JavaScript (fetch):**
```javascript
const API_URL = "http://localhost:8081";
const headers = { Authorization: "Bearer your-secret-key" };

// Get overview
const resp = await fetch(`${API_URL}/api/v1/overview/`, { headers });
const { data } = await resp.json();
console.log(`Failures: ${data.failure_stats.failed_runs}`);
console.log(`Health: ${data.dag_summary.health_score}`);
```

</details>

---

# Common

## Plugin vs API Comparison

| Aspect | Plugin (`/api/watcher`) | Standalone API (`/api/v1`) |
|--------|------------------------|---------------------------|
| **Process** | Inside Airflow webserver | Separate FastAPI process |
| **Auth** | Airflow session cookies | Bearer token |
| **RBAC** | Airflow FAB roles + `access_control` | Per-key DAG mapping via env var |
| **Install** | `pip install airflow-watcher` | `pip install airflow-watcher[standalone]` |
| **Port** | Same as Airflow (default 8080) | Separate (default 8081) |
| **UI** | 7 dashboard pages | Swagger UI at `/docs` |
| **Caching** | No built-in cache | TTL-based response cache |
| **Liveness probe** | `/api/watcher/health` | `/healthz` (no auth) |
| **Best for** | Airflow operators using the UI | External tools, CI/CD, dashboards |

<details>
<summary><h2>Project Structure</h2></summary>

```
airflow-watcher/
├── src/
│   └── airflow_watcher/
│       ├── __init__.py
│       ├── plugins/           # Airflow plugin definitions          ← Plugin
│       ├── views/             # Flask Blueprint views (plugin UI)   ← Plugin
│       ├── templates/         # Jinja2 templates (plugin UI)        ← Plugin
│       ├── api/               # Standalone FastAPI service           ← API
│       │   ├── main.py        # App entry point & create_app()
│       │   ├── auth.py        # Bearer token authentication
│       │   ├── rbac_dep.py    # RBAC dependency (per-key DAG filtering)
│       │   ├── db.py          # SQLAlchemy session management
│       │   ├── envelope.py    # Standard JSON response wrapper
│       │   ├── standalone_config.py  # Env var config loading
│       │   └── routers/       # 11 API routers
│       │       ├── failures.py
│       │       ├── sla.py
│       │       ├── tasks.py
│       │       ├── scheduling.py
│       │       ├── dags.py
│       │       ├── dependencies.py
│       │       ├── overview.py
│       │       ├── health.py
│       │       ├── alerts.py
│       │       ├── cache.py
│       │       └── metrics.py
│       ├── monitors/          # DAG & SLA monitoring logic           ← Shared
│       ├── notifiers/         # Slack, Email, PagerDuty              ← Shared
│       ├── metrics/           # Prometheus, StatsD emitters          ← Shared
│       └── utils/             # Cache, helpers, RBAC utilities       ← Shared
├── tests/
│   ├── test_routers.py        # Router endpoint tests
│   ├── test_auth.py           # Authentication tests
│   ├── test_security.py       # Penetration & security tests
│   ├── test_load.py           # Load & stress tests
│   ├── ...                    # 17 unit test files total
│   └── live/                  # Live integration tests (need Docker)
│       ├── conftest.py        # Auto-skips when containers are down
│       ├── test_qa_deep.py    # Standalone API deep QA (334 tests)
│       ├── test_qa_plugin.py  # Flask plugin API deep QA (138 tests)
│       ├── test_live_comprehensive.py
│       ├── test_live_api.py
│       └── test_live_data.py
├── demo/                      # Local demo Airflow environment
├── .env                       # Local credentials (gitignored)
└── pyproject.toml
```

</details>

<details>
<summary><h2>Testing</h2></summary>

```bash
# Unit tests (302 pass)
pytest tests/ --ignore=tests/live -v --no-cov

# Security & penetration tests only
pytest tests/test_security.py -v

# Load & stress tests only
pytest tests/test_load.py -v

# Live integration tests (requires demo Docker environment)
python tests/live/test_qa_deep.py      # Standalone API deep QA (334 tests)
python tests/live/test_qa_plugin.py     # Flask plugin API deep QA (138 tests)
python tests/live/test_live_comprehensive.py  # End-to-end integration

# Skip the pre-existing DB connectivity test
pytest tests/ -k "not test_logs_error_on_unreachable_db"
```

</details>

## Infrastructure Integration

📖 **See [INSTALL.md](INSTALL.md#infrastructure-integration) for full deployment guides:**

- **[AWS MWAA](INSTALL.md#aws-mwaa)** — `requirements.txt` + S3 upload, env var config, MWAA Local Runner testing
- **[Google Cloud Composer](INSTALL.md#google-cloud-composer)** — PyPI install via `gcloud`, Composer 1 vs 2, networking
- **[Kubernetes / Helm](INSTALL.md#kubernetes--helm-airflow-helm-chart)** — Helm values, K8s secrets, ServiceMonitor, standalone API as sidecar
- **[Production Deployment](INSTALL.md#production-deployment-standalone-api)** — Nginx, Gunicorn, systemd, Docker Compose, production checklist

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
