"""Airflow Watcher - DAG Failure and SLA Monitoring Plugin."""

try:
    from importlib.metadata import version

    __version__ = version("airflow-watcher")
except Exception:
    __version__ = "1.0.0"
