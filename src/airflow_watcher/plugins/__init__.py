"""Airflow Watcher Plugin - Main plugin registration."""

from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

from airflow_watcher.views.dashboard import WatcherDashboardView
from airflow_watcher.views.api import watcher_api_blueprint


# Create Flask Blueprint for the UI
watcher_bp = Blueprint(
    "watcher",
    __name__,
    template_folder="../templates",
    static_folder="../static",
    static_url_path="/static/watcher",
)


class AirflowWatcherPlugin(AirflowPlugin):
    """Airflow plugin for DAG monitoring and SLA tracking."""

    name = "airflow_watcher"

    # Flask blueprints for custom views
    flask_blueprints = [watcher_bp, watcher_api_blueprint]

    # AppBuilder views (for Airflow 2.x UI)
    appbuilder_views = [
        {
            "name": "Watcher Dashboard",
            "category": "Admin",
            "view": WatcherDashboardView(),
        }
    ]

    # Menu items
    appbuilder_menu_items = [
        {
            "name": "Dashboard",
            "category": "Watcher",
            "category_icon": "fa-eye",
            "href": "/watcher/dashboard",
        },
        {
            "name": "DAG Failures",
            "category": "Watcher",
            "category_icon": "fa-eye",
            "href": "/watcher/failures",
        },
        {
            "name": "SLA Tracker",
            "category": "Watcher",
            "category_icon": "fa-eye",
            "href": "/watcher/sla",
        },
        {
            "name": "Task Health",
            "category": "Watcher",
            "category_icon": "fa-eye",
            "href": "/watcher/tasks",
        },
        {
            "name": "Scheduling",
            "category": "Watcher",
            "category_icon": "fa-eye",
            "href": "/watcher/scheduling",
        },
        {
            "name": "DAG Health",
            "category": "Watcher",
            "category_icon": "fa-eye",
            "href": "/watcher/dag-health",
        },
        {
            "name": "Dependencies",
            "category": "Watcher",
            "category_icon": "fa-eye",
            "href": "/watcher/dependencies",
        },
        {
            "name": "Health Overview",
            "category": "Watcher",
            "category_icon": "fa-eye",
            "href": "/watcher/health",
        },
    ]
