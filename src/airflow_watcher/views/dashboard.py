"""Dashboard Views for Airflow Watcher."""

from flask import request
from flask_appbuilder import BaseView, expose

from airflow_watcher.monitors.dag_failure_monitor import DAGFailureMonitor
from airflow_watcher.monitors.dag_health_monitor import DAGHealthMonitor
from airflow_watcher.monitors.dependency_monitor import DependencyMonitor
from airflow_watcher.monitors.scheduling_monitor import SchedulingMonitor
from airflow_watcher.monitors.sla_monitor import SLAMonitor
from airflow_watcher.monitors.task_health_monitor import TaskHealthMonitor
from airflow_watcher.utils.helpers import get_all_dag_owners, get_all_dag_tags, get_dags_by_filter
from airflow_watcher.utils.rbac import (
    get_accessible_dag_ids,
    get_rbac_context,
    merge_rbac_with_filters,
)


def get_filter_context(request_args):
    """Extract filter parameters, apply RBAC, and get available options."""
    tag = request_args.get("tag", "").strip() or None
    owner = request_args.get("owner", "").strip() or None
    hours = int(request_args.get("hours", 24))

    # Voluntary tag/owner filter
    filter_dag_ids = get_dags_by_filter(tag=tag, owner=owner)

    # Mandatory RBAC filter — intersect with voluntary filters
    rbac_dag_ids = get_accessible_dag_ids()
    allowed_dag_ids = merge_rbac_with_filters(rbac_dag_ids, filter_dag_ids)

    return {
        "filters": {"tag": tag, "owner": owner, "hours": hours},
        "available_tags": get_all_dag_tags(),
        "available_owners": get_all_dag_owners(),
        "allowed_dag_ids": allowed_dag_ids,
        **get_rbac_context(),
    }


def filter_results(results, allowed_dag_ids, dag_id_key="dag_id"):
    """Filter results by allowed DAG IDs (RBAC + voluntary filters)."""
    if allowed_dag_ids is None:
        return results
    if isinstance(results, list):
        return [r for r in results if r.get(dag_id_key) in allowed_dag_ids]
    elif isinstance(results, dict):
        return {k: v for k, v in results.items() if k in allowed_dag_ids}
    return results


def _filter_aggregate_stats(stats, allowed_dag_ids):
    """Filter aggregate statistics dict to only count accessible DAGs.

    For stats dicts that contain a 'by_dag' or similar per-DAG breakdown,
    recompute totals from the filtered subset. For flat stats, return as-is
    when no restriction applies.
    """
    if allowed_dag_ids is None or not isinstance(stats, dict):
        return stats

    filtered = dict(stats)

    # Filter per-DAG breakdowns commonly returned by monitors
    for key in ("by_dag", "per_dag", "dag_stats", "dags"):
        if key in filtered and isinstance(filtered[key], dict):
            filtered[key] = {
                dag_id: v
                for dag_id, v in filtered[key].items()
                if dag_id in allowed_dag_ids
            }

    # Recompute total counts from filtered per-DAG data if available
    if "by_dag" in filtered and isinstance(filtered["by_dag"], dict):
        filtered["total_failures"] = sum(
            v if isinstance(v, (int, float)) else v.get("count", 0)
            for v in filtered["by_dag"].values()
        )
        filtered["affected_dags"] = len(filtered["by_dag"])

    return filtered


def _filter_dag_summary(summary, allowed_dag_ids):
    """Filter DAG status summary to only reflect accessible DAGs."""
    if allowed_dag_ids is None or not isinstance(summary, dict):
        return summary

    filtered = dict(summary)

    # Filter any per-DAG lists within the summary
    for key in ("active_dags", "paused_dags", "dags"):
        if key in filtered and isinstance(filtered[key], list):
            filtered[key] = [
                d for d in filtered[key]
                if (d.get("dag_id") if isinstance(d, dict) else d) in allowed_dag_ids
            ]

    return filtered


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
        filter_ctx = get_filter_context(request.args)
        allowed = filter_ctx["allowed_dag_ids"]

        recent_failures = filter_results(failure_monitor.get_recent_failures(limit=20), allowed)
        sla_misses = filter_results(sla_monitor.get_recent_sla_misses(limit=20), allowed)
        long_running = filter_results(task_monitor.get_long_running_tasks(threshold_minutes=60), allowed)
        zombies = filter_results(task_monitor.get_zombie_tasks(), allowed)

        # Filter aggregate stats so they only reflect accessible DAGs
        failure_stats = _filter_aggregate_stats(
            failure_monitor.get_failure_statistics(), allowed
        )
        sla_stats = _filter_aggregate_stats(
            sla_monitor.get_sla_statistics(), allowed
        )
        dag_summary = _filter_dag_summary(
            dag_monitor.get_dag_status_summary(), allowed
        )

        context = {
            "recent_failures": recent_failures,
            "failure_stats": failure_stats,
            "sla_misses": sla_misses,
            "sla_stats": sla_stats,
            "long_running_count": len(long_running),
            "zombie_count": len(zombies),
            "queue_status": scheduling_monitor.get_queued_tasks(),
            "dag_summary": dag_summary,
            "import_error_count": len(dag_monitor.get_dag_import_errors()),
            "show_hours_filter": False,
            **filter_ctx,
        }
        return self.render_template("watcher/dashboard.html", **context)

    @expose("/failures")
    def failures(self):
        """View for DAG failures."""
        failure_monitor = DAGFailureMonitor()
        filter_ctx = get_filter_context(request.args)

        # Get filter parameters
        dag_id = request.args.get("dag_id")
        hours = filter_ctx["filters"]["hours"]

        failures = failure_monitor.get_recent_failures(dag_id=dag_id, lookback_hours=hours, limit=100)
        stats = failure_monitor.get_failure_statistics(lookback_hours=hours)

        # Apply tag/owner filter
        failures = filter_results(failures, filter_ctx["allowed_dag_ids"])

        context = {
            "failures": failures,
            "stats": stats,
            "show_hours_filter": True,
            **filter_ctx,
        }
        return self.render_template("watcher/failures.html", **context)

    @expose("/sla")
    def sla(self):
        """View for SLA misses and delays."""
        sla_monitor = SLAMonitor()
        filter_ctx = get_filter_context(request.args)

        # Get filter parameters
        dag_id = request.args.get("dag_id")
        hours = filter_ctx["filters"]["hours"]

        sla_misses = sla_monitor.get_recent_sla_misses(dag_id=dag_id, lookback_hours=hours, limit=100)
        sla_stats = sla_monitor.get_sla_statistics(lookback_hours=hours)

        # Apply tag/owner filter
        sla_misses = filter_results(sla_misses, filter_ctx["allowed_dag_ids"])

        context = {
            "sla_misses": sla_misses,
            "stats": sla_stats,
            "show_hours_filter": True,
            **filter_ctx,
        }
        return self.render_template("watcher/sla.html", **context)

    @expose("/health")
    def health(self):
        """DAG health overview - merged with DAG health monitoring."""
        failure_monitor = DAGFailureMonitor()
        scheduling_monitor = SchedulingMonitor()
        dag_monitor = DAGHealthMonitor()
        filter_ctx = get_filter_context(request.args)
        allowed = filter_ctx["allowed_dag_ids"]

        health_status = failure_monitor.get_dag_health_status()
        scheduling_lag = scheduling_monitor.get_scheduling_lag(lookback_hours=24)
        stale_dags = scheduling_monitor.get_stale_dags(expected_interval_hours=24)

        # Apply filters
        if allowed is not None:
            health_status["healthy"] = [d for d in health_status.get("healthy", []) if d.get("dag_id") in allowed]
            health_status["unhealthy"] = [d for d in health_status.get("unhealthy", []) if d.get("dag_id") in allowed]
            health_status["summary"]["healthy_count"] = len(health_status["healthy"])
            health_status["summary"]["unhealthy_count"] = len(health_status["unhealthy"])

        delayed_dags = filter_results(scheduling_lag.get("delayed_dags", []), allowed)
        stale_dags = filter_results(stale_dags, allowed)
        import_errors = dag_monitor.get_dag_import_errors()
        inactive_dags = filter_results(dag_monitor.get_inactive_dags(inactive_days=30), allowed)

        context = {
            "health_status": health_status,
            "delayed_dags": delayed_dags,
            "scheduling_summary": scheduling_lag,
            "stale_dags": stale_dags,
            "import_errors": import_errors,
            "inactive_dags": inactive_dags,
            "show_hours_filter": False,
            **filter_ctx,
        }
        return self.render_template("watcher/health.html", **context)

    @expose("/tasks")
    def task_health(self):
        """Task health monitoring view."""
        task_monitor = TaskHealthMonitor()
        filter_ctx = get_filter_context(request.args)
        allowed = filter_ctx["allowed_dag_ids"]

        long_running = filter_results(task_monitor.get_long_running_tasks(threshold_minutes=60), allowed)
        retry_tasks = filter_results(task_monitor.get_retry_heavy_tasks(lookback_hours=24), allowed)
        zombies = filter_results(task_monitor.get_zombie_tasks(), allowed)
        failure_patterns = filter_results(task_monitor.get_task_failure_patterns(lookback_hours=168), allowed)

        context = {
            "long_running_tasks": long_running,
            "retry_tasks": retry_tasks,
            "zombies": zombies,
            "failure_patterns": failure_patterns,
            "show_hours_filter": False,
            **filter_ctx,
        }
        return self.render_template("watcher/task_health.html", **context)

    @expose("/scheduling")
    def scheduling(self):
        """Scheduling and queue monitoring view."""
        scheduling_monitor = SchedulingMonitor()
        filter_ctx = get_filter_context(request.args)
        allowed = filter_ctx["allowed_dag_ids"]

        scheduling_lag = scheduling_monitor.get_scheduling_lag(lookback_hours=24)
        stale_dags = filter_results(scheduling_monitor.get_stale_dags(expected_interval_hours=24), allowed)

        # Filter delayed dags
        if allowed is not None and "delayed_dags" in scheduling_lag:
            scheduling_lag["delayed_dags"] = [d for d in scheduling_lag["delayed_dags"] if d.get("dag_id") in allowed]

        context = {
            "scheduling_lag": scheduling_lag,
            "queue_status": scheduling_monitor.get_queued_tasks(),
            "pools": scheduling_monitor.get_pool_utilization(),
            "stale_dags": stale_dags,
            "concurrent_runs": scheduling_monitor.get_concurrent_runs(),
            "show_hours_filter": False,
            **filter_ctx,
        }
        return self.render_template("watcher/scheduling.html", **context)

    @expose("/dag-health")
    def dag_health_view(self):
        """DAG-level health monitoring view."""
        dag_monitor = DAGHealthMonitor()
        filter_ctx = get_filter_context(request.args)
        allowed = filter_ctx["allowed_dag_ids"]

        inactive_dags = filter_results(dag_monitor.get_inactive_dags(inactive_days=30), allowed)
        complexity = dag_monitor.get_dag_complexity_analysis()
        if allowed is not None:
            complexity = [c for c in complexity if c.get("dag_id") in allowed]

        context = {
            "dag_summary": dag_monitor.get_dag_status_summary(),
            "import_errors": dag_monitor.get_dag_import_errors(),
            "complexity": complexity,
            "inactive_dags": inactive_dags,
            "show_hours_filter": False,
            **filter_ctx,
        }
        return self.render_template("watcher/dag_health.html", **context)

    @expose("/dependencies")
    def dependencies(self):
        """Dependency monitoring view."""
        dep_monitor = DependencyMonitor()
        filter_ctx = get_filter_context(request.args)
        allowed = filter_ctx["allowed_dag_ids"]

        upstream_failures = filter_results(dep_monitor.get_upstream_failures(lookback_hours=24), allowed)
        correlations = dep_monitor.get_failure_correlation(lookback_hours=24)

        context = {
            "upstream_failures": upstream_failures,
            "cross_dag_deps": dep_monitor.get_cross_dag_dependencies(),
            "correlations": correlations,
            "show_hours_filter": False,
            **filter_ctx,
        }
        return self.render_template("watcher/dependencies.html", **context)
