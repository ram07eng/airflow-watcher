"""Dashboard Views for Airflow Watcher."""

from flask import Blueprint, render_template, jsonify, request
from flask_appbuilder import BaseView, expose

from airflow_watcher.monitors.dag_failure_monitor import DAGFailureMonitor
from airflow_watcher.monitors.sla_monitor import SLAMonitor


class WatcherDashboardView(BaseView):
    """Main dashboard view for DAG monitoring."""

    route_base = "/watcher"
    default_view = "dashboard"

    @expose("/dashboard")
    def dashboard(self):
        """Render the main monitoring dashboard."""
        failure_monitor = DAGFailureMonitor()
        sla_monitor = SLAMonitor()

        context = {
            "recent_failures": failure_monitor.get_recent_failures(limit=20),
            "failure_stats": failure_monitor.get_failure_statistics(),
            "sla_misses": sla_monitor.get_recent_sla_misses(limit=20),
            "sla_stats": sla_monitor.get_sla_statistics(),
        }
        return self.render_template("watcher/dashboard.html", **context)

    @expose("/failures")
    def failures(self):
        """View for DAG failures."""
        failure_monitor = DAGFailureMonitor()
        
        # Get filter parameters
        dag_id = request.args.get("dag_id")
        hours = int(request.args.get("hours", 24))

        failures = failure_monitor.get_recent_failures(
            dag_id=dag_id,
            lookback_hours=hours,
            limit=100
        )
        stats = failure_monitor.get_failure_statistics(lookback_hours=hours)

        context = {
            "failures": failures,
            "stats": stats,
            "filters": {"dag_id": dag_id, "hours": hours},
        }
        return self.render_template("watcher/failures.html", **context)

    @expose("/sla")
    def sla(self):
        """View for SLA misses and delays."""
        sla_monitor = SLAMonitor()

        # Get filter parameters
        dag_id = request.args.get("dag_id")
        hours = int(request.args.get("hours", 24))

        sla_misses = sla_monitor.get_recent_sla_misses(
            dag_id=dag_id,
            lookback_hours=hours,
            limit=100
        )
        sla_stats = sla_monitor.get_sla_statistics(lookback_hours=hours)

        context = {
            "sla_misses": sla_misses,
            "stats": sla_stats,
            "filters": {"dag_id": dag_id, "hours": hours},
        }
        return self.render_template("watcher/sla.html", **context)

    @expose("/health")
    def health(self):
        """DAG health overview."""
        failure_monitor = DAGFailureMonitor()
        
        health_status = failure_monitor.get_dag_health_status()
        
        context = {
            "health_status": health_status,
        }
        return self.render_template("watcher/health.html", **context)
