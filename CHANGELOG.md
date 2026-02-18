# Changelog

All notable changes to this project will be documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- GitHub Actions CI workflow (lint, test, type-check, build across Python 3.9–3.12)
- `CONTRIBUTING.md` with development setup and PR guidelines
- `CHANGELOG.md`
- Proper `/api/watcher/health` endpoint returning HTTP 200/503 based on health score
- Python 3.12 classifier in `pyproject.toml`
- `Topic :: System :: Monitoring` classifier
- Weighted, configurable health score in `DAGHealthMonitor._calculate_health_score`
- Tests for `MetricsCollector`, `DAGHealthMonitor`, and `PagerDutyNotifier`
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
