"""Monitors package for Airflow Watcher."""

from airflow_watcher.monitors.dag_failure_monitor import DAGFailureMonitor
from airflow_watcher.monitors.sla_monitor import SLAMonitor

__all__ = ["DAGFailureMonitor", "SLAMonitor"]
