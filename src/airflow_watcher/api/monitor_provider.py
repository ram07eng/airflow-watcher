"""Lazy-initialized shared monitor instances for the API layer.

Avoids duplicate monitor instances across routers and defers creation until
first use (after config and DB are fully initialised).
"""

import threading
from typing import Optional

_lock = threading.Lock()

_failure_monitor = None
_sla_monitor = None
_task_monitor = None
_scheduling_monitor = None
_dag_health_monitor = None
_dependency_monitor = None
_config = None


def _get_config():
    """Return a config object with env-var-loaded settings.

    Uses StandaloneConfig (no Airflow dependency) so monitors receive
    the actual values from AIRFLOW_WATCHER_* environment variables
    rather than WatcherConfig defaults.
    """
    global _config
    if _config is None:
        from airflow_watcher.api.standalone_config import StandaloneConfig
        try:
            _config = StandaloneConfig.from_env()
        except (ValueError, SystemExit):
            # Fallback: if standalone config fails (e.g. missing DB_URI in
            # test context), create a minimal WatcherConfig with env overrides.
            from airflow_watcher.config import WatcherConfig
            _config = WatcherConfig()
            _config._load_from_env()
    return _config


def get_failure_monitor():
    global _failure_monitor
    if _failure_monitor is None:
        with _lock:
            if _failure_monitor is None:
                from airflow_watcher.monitors.dag_failure_monitor import DAGFailureMonitor

                _failure_monitor = DAGFailureMonitor(config=_get_config())
    return _failure_monitor


def get_sla_monitor():
    global _sla_monitor
    if _sla_monitor is None:
        with _lock:
            if _sla_monitor is None:
                from airflow_watcher.monitors.sla_monitor import SLAMonitor

                _sla_monitor = SLAMonitor(config=_get_config())
    return _sla_monitor


def get_task_monitor():
    global _task_monitor
    if _task_monitor is None:
        with _lock:
            if _task_monitor is None:
                from airflow_watcher.monitors.task_health_monitor import TaskHealthMonitor

                _task_monitor = TaskHealthMonitor(config=_get_config())
    return _task_monitor


def get_scheduling_monitor():
    global _scheduling_monitor
    if _scheduling_monitor is None:
        with _lock:
            if _scheduling_monitor is None:
                from airflow_watcher.monitors.scheduling_monitor import SchedulingMonitor

                _scheduling_monitor = SchedulingMonitor(config=_get_config())
    return _scheduling_monitor


def get_dag_health_monitor():
    global _dag_health_monitor
    if _dag_health_monitor is None:
        with _lock:
            if _dag_health_monitor is None:
                from airflow_watcher.monitors.dag_health_monitor import DAGHealthMonitor

                _dag_health_monitor = DAGHealthMonitor(config=_get_config())
    return _dag_health_monitor


def get_dependency_monitor():
    global _dependency_monitor
    if _dependency_monitor is None:
        with _lock:
            if _dependency_monitor is None:
                from airflow_watcher.monitors.dependency_monitor import DependencyMonitor

                _dependency_monitor = DependencyMonitor(config=_get_config())
    return _dependency_monitor
