"""Views package for Airflow Watcher."""

from airflow_watcher.views.dashboard import WatcherDashboardView
from airflow_watcher.views.api import watcher_api_blueprint

__all__ = ["WatcherDashboardView", "watcher_api_blueprint"]
