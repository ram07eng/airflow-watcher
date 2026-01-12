"""Dashboard Views for Airflow Watcher."""

from flask import Blueprint, render_template, jsonify, request
from flask_appbuilder import BaseView, expose

from airflow_watcher.monitors.dag_failure_monitor import DAGFailureMonitor
from airflow_watcher.monitors.sla_monitor import SLAMonitor
from airflow_watcher.monitors.task_health_monitor import TaskHealthMonitor
from airflow_watcher.monitors.scheduling_monitor import SchedulingMonitor
from airflow_watcher.monitors.dag_health_monitor import DAGHealthMonitor
from airflow_watcher.monitors.dependency_monitor import DependencyMonitor


class WatcherDashboardView(BaseView):
    """Main dashboard view for DAG monitoring."""

    route_base = "/watcher"
    default_view = "dashboard"

    @expose("/dashboard")
    def dashboard(self):
        """Render the main monitoring dashboard."""
        failure_monitor = DAGFailureMonitor()
        sla_monitor = SLAMonitor()
        task_monitor = TaskHealthMonitor()
        scheduling_monitor = SchedulingMonitor()
        dag_monitor = DAGHealthMonitor()

        context = {
            "recent_failures": failure_monitor.get_recent_failures(limit=20),
            "failure_stats": failure_monitor.get_failure_statistics(),
            "sla_misses": sla_monitor.get_recent_sla_misses(limit=20),
            "sla_stats": sla_monitor.get_sla_statistics(),
            "long_running_count": len(task_monitor.get_long_running_tasks(threshold_minutes=60)),
            "zombie_count": len(task_monitor.get_zombie_tasks()),
            "queue_status": scheduling_monitor.get_queued_tasks(),
            "dag_summary": dag_monitor.get_dag_status_summary(),
            "import_error_count": len(dag_monitor.get_dag_import_errors()),
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

    @expose("/tasks")
    def task_health(self):
        """Task health monitoring view."""
        task_monitor = TaskHealthMonitor()
        
        context = {
            "long_running_tasks": task_monitor.get_long_running_tasks(threshold_minutes=60),
            "retry_tasks": task_monitor.get_retry_heavy_tasks(lookback_hours=24),
            "zombies": task_monitor.get_zombie_tasks(),
            "failure_patterns": task_monitor.get_task_failure_patterns(lookback_hours=168),
        }
        return self.render_template("watcher/task_health.html", **context)

    @expose("/scheduling")
    def scheduling(self):
        """Scheduling and queue monitoring view."""
        scheduling_monitor = SchedulingMonitor()
        
        context = {
            "scheduling_lag": scheduling_monitor.get_scheduling_lag(lookback_hours=24),
            "queue_status": scheduling_monitor.get_queued_tasks(),
            "pools": scheduling_monitor.get_pool_utilization(),
            "stale_dags": scheduling_monitor.get_stale_dags(expected_interval_hours=24),
            "concurrent_runs": scheduling_monitor.get_concurrent_runs(),
        }
        return self.render_template("watcher/scheduling.html", **context)

    @expose("/dag-health")
    def dag_health_view(self):
        """DAG-level health monitoring view."""
        dag_monitor = DAGHealthMonitor()
        
        context = {
            "dag_summary": dag_monitor.get_dag_status_summary(),
            "import_errors": dag_monitor.get_dag_import_errors(),
            "complexity": dag_monitor.get_dag_complexity_analysis(),
            "inactive_dags": dag_monitor.get_inactive_dags(inactive_days=30),
        }
        return self.render_template("watcher/dag_health.html", **context)

    @expose("/dependencies")
    def dependencies(self):
        """Dependency monitoring view."""
        dep_monitor = DependencyMonitor()
        
        context = {
            "upstream_failures": dep_monitor.get_upstream_failures(lookback_hours=24),
            "cross_dag_deps": dep_monitor.get_cross_dag_dependencies(),
            "correlations": dep_monitor.get_failure_correlation(lookback_hours=24),
        }
        return self.render_template("watcher/dependencies.html", **context)
