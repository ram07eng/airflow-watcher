# Changelog

All notable changes to this project will be documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Offset-based pagination (`offset` query parameter) on `/failures/` and `/sla/misses` endpoints (both standalone API and plugin)
- Structured JSON logging (`AIRFLOW_WATCHER_LOG_FORMAT=json`) with per-request `X-Request-ID` header
- Configurable log level (`AIRFLOW_WATCHER_LOG_LEVEL`)
- Startup schema validation in `compat.py` — logs error if critical Airflow tables (dag_run, task_instance, dag) cannot be reflected
- API versioning strategy documented (URL-prefix `/api/v1/`, `X-API-Version` header, future `Sunset` header plan)
- Tests for pagination, structured logging, and compat validation

### Fixed
- Console script entry point (`airflow-watcher-api`) and Gunicorn module (`main:app`) now install Airflow stubs and reflect models automatically — previously only `run_api.py` did this
- Error response envelope uses `"error"` field consistently (was `"message"`)

## [2.0.0] - 2026-04-10

### Added — Standalone REST API
- **Standalone FastAPI service** — run Airflow Watcher outside the webserver as an independent process
- 11 API routers: failures, SLA, tasks, scheduling, DAGs, dependencies, overview, health, alerts, cache, metrics
- Bearer-token authentication with constant-time comparison (`secrets.compare_digest`)
- Per-key RBAC — map API keys to specific DAGs via `AIRFLOW_WATCHER_RBAC_KEY_DAG_MAPPING`
- Thread-safe TTL response cache (`MetricsCache`) with configurable `CACHE_TTL`
- Per-IP sliding-window rate limiting (`RateLimitMiddleware`, default 120 req/min)
- Read-replica database support (`AIRFLOW_WATCHER_DB_READ_URI`)
- Connection-pool tuning (`DB_POOL_SIZE`, `DB_MAX_OVERFLOW`)
- PostgreSQL query timeout (`QUERY_TIMEOUT_MS`, default 30 s)
- `/healthz` liveness probe (no auth, checks primary + read-replica DB)
- Prometheus `/metrics` endpoint (standalone mode)
- Swagger UI at `/docs` and hosted at GitHub Pages
- Console script entry point `airflow-watcher-api`
- Module-level `app` for Gunicorn (`airflow_watcher.api.main:app`)
- Security headers middleware (CSP, X-Frame-Options, nosniff, no-store)
- Request timeout middleware (60 s hard limit)
- X-API-Version response header

### Added — Plugin
- Proper `/api/watcher/health` endpoint returning HTTP 200/503 based on health score
- Weighted, configurable health score in `DAGHealthMonitor._calculate_health_score`
- Python 3.12 classifier in `pyproject.toml`
- `Topic :: System :: Monitoring` classifier

### Added — Project
- GitHub Actions CI workflow (lint, test, type-check, build across Python 3.10–3.12)
- `CONTRIBUTING.md` with development setup and PR guidelines
- `CHANGELOG.md`
- `INSTALL.md` — unified installation guide for plugin, API, and infrastructure
- `ALERTING.md` — alerting & metrics configuration guide
- `requirements-api.txt` for standalone API dependencies
- Docker Compose demo with three Airflow instances and standalone API
- Tests for `MetricsCollector`, `DAGHealthMonitor`, `PagerDutyNotifier`, auth, RBAC, rate limiting, DB, standalone config
- Airflow stub in `conftest.py` so tests run without a full Airflow install

### Fixed
- `MetricsCollector.collect()` was calling non-existent monitor methods
  (`get_sla_misses`, `get_task_stats`, `get_missed_schedules`, `get_delayed_dags`,
  `get_unhealthy_dags`, `get_all_dag_health`, `get_failed_sensors`, `get_blocked_dags`).
  All calls now use the correct public API of each monitor.
- `PagerDutyNotifier.send_failure_alert()` was accessing non-existent fields
  (`task_id`, `error_message`, `try_number`, `owner`) on `DAGFailure`.
- `PagerDutyNotifier.send_sla_miss_alert()` was accessing non-existent fields
  (`expected_completion`, `actual_completion`, `delay_minutes`) on `SLAMissEvent`.
- `conftest.py` `mock_airflow_config` fixture was a no-op (`pass`).
- Tightened Airflow dependency to `>=2.7.0,<3.0` to reflect tested range.

### Compatibility
- **Python**: Requires 3.10+ (changed from 3.9+ in 0.1.0)
- **Airflow**: Tested with 2.7.x–2.10.x. Airflow 3.x is **not yet supported** — see README for details.

## [0.1.0] - 2024-01-01

### Added
- Initial release
- DAG failure monitoring with Slack, Email, and PagerDuty notifications
- SLA miss detection and alerting
- Dashboard UI plugin for Airflow 2.x
- Scheduling lag and queue health monitoring
- Task health monitoring (long-running, zombie, retry-heavy tasks)
- Cross-DAG dependency tracking
- Prometheus metrics endpoint
- StatsD/Datadog metrics emission
- Configurable alert rules with pre-defined templates
- Docker Compose demo environment
