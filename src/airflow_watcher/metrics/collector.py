"""Metrics Collector - Gathers metrics from monitors for emission."""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional

from airflow.utils import timezone

logger = logging.getLogger(__name__)


@dataclass
class WatcherMetrics:
    """Container for all Watcher metrics."""

    # Failure metrics
    total_failures_24h: int = 0
    unique_dag_failures_24h: int = 0
    failure_rate_percent: float = 0.0

    # SLA metrics
    total_sla_misses_24h: int = 0
    sla_miss_rate_percent: float = 0.0

    # Task health metrics
    failed_tasks_24h: int = 0
    retry_tasks_24h: int = 0
    long_running_tasks: int = 0

    # Scheduling metrics
    missed_schedules_24h: int = 0
    delayed_dags: int = 0

    # DAG health metrics
    unhealthy_dags: int = 0
    total_dags: int = 0
    healthy_dag_percent: float = 0.0

    # Dependency metrics
    failed_sensors: int = 0
    blocked_dags: int = 0

    # Timestamps
    collected_at: datetime = field(default_factory=timezone.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary for emission."""
        return {
            "failures.total_24h": self.total_failures_24h,
            "failures.unique_dags_24h": self.unique_dag_failures_24h,
            "failures.rate_percent": self.failure_rate_percent,
            "sla.misses_24h": self.total_sla_misses_24h,
            "sla.miss_rate_percent": self.sla_miss_rate_percent,
            "tasks.failed_24h": self.failed_tasks_24h,
            "tasks.retry_24h": self.retry_tasks_24h,
            "tasks.long_running": self.long_running_tasks,
            "scheduling.missed_24h": self.missed_schedules_24h,
            "scheduling.delayed_dags": self.delayed_dags,
            "dags.unhealthy": self.unhealthy_dags,
            "dags.total": self.total_dags,
            "dags.healthy_percent": self.healthy_dag_percent,
            "dependencies.failed_sensors": self.failed_sensors,
            "dependencies.blocked_dags": self.blocked_dags,
        }


class MetricsCollector:
    """Collects metrics from all monitors."""

    def __init__(self):
        """Initialize the metrics collector."""
        self._last_metrics: Optional[WatcherMetrics] = None
        self._collection_interval_seconds = 60

    def collect(self) -> WatcherMetrics:
        """Collect metrics from all monitors.

        Returns:
            WatcherMetrics with current values
        """
        from airflow_watcher.monitors.dag_failure_monitor import DAGFailureMonitor
        from airflow_watcher.monitors.dag_health_monitor import DAGHealthMonitor
        from airflow_watcher.monitors.dependency_monitor import DependencyMonitor
        from airflow_watcher.monitors.scheduling_monitor import SchedulingMonitor
        from airflow_watcher.monitors.sla_monitor import SLAMonitor
        from airflow_watcher.monitors.task_health_monitor import TaskHealthMonitor

        metrics = WatcherMetrics()

        try:
            # Failure metrics
            failure_monitor = DAGFailureMonitor()
            failures = failure_monitor.get_recent_failures(lookback_hours=24)
            metrics.total_failures_24h = len(failures)
            metrics.unique_dag_failures_24h = len(set(f.dag_id for f in failures))

            # SLA metrics
            sla_monitor = SLAMonitor()
            sla_misses = sla_monitor.get_recent_sla_misses(lookback_hours=24)
            metrics.total_sla_misses_24h = len(sla_misses)

            # Task health metrics
            task_monitor = TaskHealthMonitor()
            failure_patterns = task_monitor.get_task_failure_patterns(lookback_hours=24)
            metrics.failed_tasks_24h = failure_patterns.get("total_failures", 0)
            retry_tasks = task_monitor.get_retry_heavy_tasks(lookback_hours=24, min_retries=1)
            metrics.retry_tasks_24h = len(retry_tasks)
            long_running = task_monitor.get_long_running_tasks(threshold_minutes=60)
            metrics.long_running_tasks = len(long_running)

            # Scheduling metrics
            scheduling_monitor = SchedulingMonitor()
            scheduling_lag = scheduling_monitor.get_scheduling_lag(
                lookback_hours=24, lag_threshold_minutes=10
            )
            metrics.missed_schedules_24h = scheduling_lag.get("delayed_count", 0)
            stale = scheduling_monitor.get_stale_dags(expected_interval_hours=24)
            metrics.delayed_dags = len(stale)

            # DAG health metrics
            health_monitor = DAGHealthMonitor()
            dag_summary = health_monitor.get_dag_status_summary()
            metrics.total_dags = dag_summary.get("total_dags", 0)
            metrics.unhealthy_dags = dag_summary.get("recent_failures_24h", 0)
            if metrics.total_dags > 0:
                healthy = metrics.total_dags - metrics.unhealthy_dags
                metrics.healthy_dag_percent = round((healthy / metrics.total_dags) * 100, 2)

            # Dependency metrics
            dep_monitor = DependencyMonitor()
            upstream_failures = dep_monitor.get_upstream_failures(lookback_hours=24)
            metrics.failed_sensors = len(upstream_failures)
            correlations = dep_monitor.get_failure_correlation(lookback_hours=24)
            metrics.blocked_dags = len(correlations.get("correlations", []))

            # Calculate failure rate
            if metrics.total_dags > 0:
                metrics.failure_rate_percent = round(
                    (metrics.unique_dag_failures_24h / metrics.total_dags) * 100, 2
                )

            metrics.collected_at = timezone.utcnow()
            self._last_metrics = metrics

        except Exception as e:
            logger.error(f"Error collecting metrics: {e}")
            if self._last_metrics:
                return self._last_metrics

        return metrics

    def get_last_metrics(self) -> Optional[WatcherMetrics]:
        """Get the last collected metrics without re-collecting."""
        return self._last_metrics
