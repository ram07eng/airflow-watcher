"""Metrics Collector - Gathers metrics from monitors for emission."""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field

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
        from airflow_watcher.monitors import (
            DAGFailureMonitor,
            SLAMonitor,
            TaskHealthMonitor,
            SchedulingMonitor,
            DAGHealthMonitor,
            DependencyMonitor,
        )
        
        metrics = WatcherMetrics()
        
        try:
            # Failure metrics
            failure_monitor = DAGFailureMonitor()
            failures = failure_monitor.get_recent_failures(hours=24)
            metrics.total_failures_24h = len(failures)
            metrics.unique_dag_failures_24h = len(set(f.dag_id for f in failures))
            
            # SLA metrics
            sla_monitor = SLAMonitor()
            sla_misses = sla_monitor.get_sla_misses(hours=24)
            metrics.total_sla_misses_24h = len(sla_misses)
            
            # Task health metrics
            task_monitor = TaskHealthMonitor()
            task_stats = task_monitor.get_task_stats(hours=24)
            metrics.failed_tasks_24h = task_stats.get("failed", 0)
            metrics.retry_tasks_24h = task_stats.get("up_for_retry", 0)
            long_running = task_monitor.get_long_running_tasks(threshold_minutes=60)
            metrics.long_running_tasks = len(long_running)
            
            # Scheduling metrics
            scheduling_monitor = SchedulingMonitor()
            missed = scheduling_monitor.get_missed_schedules(hours=24)
            metrics.missed_schedules_24h = len(missed)
            delayed = scheduling_monitor.get_delayed_dags(threshold_minutes=30)
            metrics.delayed_dags = len(delayed)
            
            # DAG health metrics
            health_monitor = DAGHealthMonitor()
            unhealthy = health_monitor.get_unhealthy_dags()
            metrics.unhealthy_dags = len(unhealthy)
            all_dags = health_monitor.get_all_dag_health()
            metrics.total_dags = len(all_dags)
            if metrics.total_dags > 0:
                healthy = metrics.total_dags - metrics.unhealthy_dags
                metrics.healthy_dag_percent = (healthy / metrics.total_dags) * 100
            
            # Dependency metrics
            dep_monitor = DependencyMonitor()
            failed_sensors = dep_monitor.get_failed_sensors()
            metrics.failed_sensors = len(failed_sensors)
            blocked = dep_monitor.get_blocked_dags()
            metrics.blocked_dags = len(blocked)
            
            # Calculate rates
            if metrics.total_dags > 0:
                metrics.failure_rate_percent = (
                    metrics.unique_dag_failures_24h / metrics.total_dags
                ) * 100
            
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
