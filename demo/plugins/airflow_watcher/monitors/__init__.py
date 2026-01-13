"""Monitors package for Airflow Watcher."""

from airflow_watcher.monitors.dag_failure_monitor import DAGFailureMonitor
from airflow_watcher.monitors.sla_monitor import SLAMonitor
from airflow_watcher.monitors.task_health_monitor import TaskHealthMonitor
from airflow_watcher.monitors.scheduling_monitor import SchedulingMonitor
from airflow_watcher.monitors.dag_health_monitor import DAGHealthMonitor
from airflow_watcher.monitors.dependency_monitor import DependencyMonitor

__all__ = [
    "DAGFailureMonitor",
    "SLAMonitor",
    "TaskHealthMonitor",
    "SchedulingMonitor",
    "DAGHealthMonitor",
    "DependencyMonitor",
]
