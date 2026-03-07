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
        return [
            r for r in results
            if (r.get(dag_id_key) if isinstance(r, dict) else getattr(r, dag_id_key, None)) in allowed_dag_ids
        ]
    elif isinstance(results, dict):
        return {k: v for k, v in results.items() if k in allowed_dag_ids}
    return results


def _filter_aggregate_stats(stats, allowed_dag_ids):
    """Filter aggregate statistics dict to only count accessible DAGs.

    Filters per-DAG breakdowns and recomputes totals from the filtered subset.
    """
    if allowed_dag_ids is None or not isinstance(stats, dict):
        return stats

    filtered = dict(stats)

    # Filter all per-DAG breakdowns (lists and dicts)
    dag_list_keys = (
        "by_dag", "per_dag", "dag_stats", "dags",
        "most_failing_dags", "top_dags_with_misses", "top_tasks_with_misses",
    )
    for key in dag_list_keys:
        if key not in filtered:
            continue
        val = filtered[key]
        if isinstance(val, dict):
            filtered[key] = {
                dag_id: v for dag_id, v in val.items()
                if dag_id in allowed_dag_ids
            }
        elif isinstance(val, list):
            filtered[key] = [
                item for item in val
                if _get_dag_id(item) in allowed_dag_ids
            ]

    # Recompute failure stats from most_failing_dags
    if "most_failing_dags" in filtered and isinstance(filtered["most_failing_dags"], list):
        total_failures = sum(
            d.get("failure_count", 0) for d in filtered["most_failing_dags"] if isinstance(d, dict)
        )
        filtered["failed_runs"] = total_failures
        filtered["total_task_failures"] = total_failures
        # We can't know exact success_runs per-DAG, so derive from total
        orig_total = stats.get("total_runs", 0)
        orig_failed = stats.get("failed_runs", 0)
        orig_success = stats.get("success_runs", 0)
        # Approximate: remove proportional success runs too
        if orig_total > 0 and orig_failed > 0:
            # Ratio of removed DAGs' runs to total
            ratio_kept = total_failures / orig_failed if orig_failed else 0
            filtered["success_runs"] = round(orig_success * ratio_kept)
            filtered["total_runs"] = total_failures + filtered["success_runs"]
        else:
            filtered["success_runs"] = 0
            filtered["total_runs"] = total_failures
        filtered["failure_rate"] = round(
            total_failures / filtered["total_runs"] * 100, 2
        ) if filtered["total_runs"] else 0

    # Recompute from by_dag dict
    if "by_dag" in filtered and isinstance(filtered["by_dag"], dict):
        filtered["total_failures"] = sum(
            v if isinstance(v, (int, float)) else v.get("count", 0)
            for v in filtered["by_dag"].values()
        )
        filtered["affected_dags"] = len(filtered["by_dag"])

    # Recompute SLA stats from top_dags_with_misses
    if "top_dags_with_misses" in filtered and isinstance(filtered["top_dags_with_misses"], list):
        filtered["total_sla_misses"] = sum(
            d.get("miss_count", 0) for d in filtered["top_dags_with_misses"] if isinstance(d, dict)
        )
        # Also recompute notifications_sent proportionally
        orig_misses = stats.get("total_sla_misses", 0)
        if orig_misses > 0:
            ratio = filtered["total_sla_misses"] / orig_misses
            if "notifications_sent" in filtered:
                filtered["notifications_sent"] = round(stats.get("notifications_sent", 0) * ratio)

    return filtered


def _get_dag_id(item):
    """Extract dag_id from a dict or object."""
    if isinstance(item, dict):
        return item.get("dag_id")
    return getattr(item, "dag_id", None)


def _filter_dag_summary(summary, allowed_dag_ids):
    """Filter DAG status summary to only reflect accessible DAGs.

    Recomputes flat counts (total_dags, active_dags, etc.) when RBAC is active.
    """
    if allowed_dag_ids is None or not isinstance(summary, dict):
        return summary

    filtered = dict(summary)

    # Filter any per-DAG lists within the summary
    for key in ("active_dags", "paused_dags", "dags"):
        if key in filtered and isinstance(filtered[key], list):
            filtered[key] = [
                d for d in filtered[key]
                if _get_dag_id(d) in allowed_dag_ids
            ]

    # Recompute flat counts to match the user's accessible DAGs
    num_allowed = len(allowed_dag_ids)
    filtered["total_dags"] = num_allowed
    # Scale proportionally — we don't have per-DAG active/paused breakdown,
    # so set active = allowed count (best approximation)
    filtered["active_dags"] = num_allowed
    filtered["paused_dags"] = 0

    # Scale other counts proportionally based on DAG ratio
    orig_total = summary.get("total_dags", 1) or 1
    ratio = num_allowed / orig_total

    for key in ("currently_running", "recent_failures_24h", "dags_with_errors"):
        if key in filtered and isinstance(filtered[key], (int, float)):
            filtered[key] = round(filtered[key] * ratio)

    # Recalculate health score based on filtered data
    if num_allowed > 0:
        failures = filtered.get("recent_failures_24h", 0)
        errors = filtered.get("dags_with_errors", 0)
        penalty = (failures * 3) + (errors * 10)
        filtered["health_score"] = max(0, min(100, 100 - penalty))

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

        # Apply RBAC + tag/owner filter
        failures = filter_results(failures, filter_ctx["allowed_dag_ids"])
        stats = _filter_aggregate_stats(stats, filter_ctx["allowed_dag_ids"])

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

        # Apply RBAC + tag/owner filter
        sla_misses = filter_results(sla_misses, filter_ctx["allowed_dag_ids"])
        sla_stats = _filter_aggregate_stats(sla_stats, filter_ctx["allowed_dag_ids"])

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
        concurrent_runs = scheduling_monitor.get_concurrent_runs()

        # Filter delayed dags and concurrent runs
        if allowed is not None:
            if "delayed_dags" in scheduling_lag:
                scheduling_lag["delayed_dags"] = [
                    d for d in scheduling_lag["delayed_dags"] if _get_dag_id(d) in allowed
                ]
            if isinstance(concurrent_runs, list):
                concurrent_runs = [c for c in concurrent_runs if _get_dag_id(c) in allowed]

        context = {
            "scheduling_lag": scheduling_lag,
            "queue_status": scheduling_monitor.get_queued_tasks(),
            "pools": scheduling_monitor.get_pool_utilization(),
            "stale_dags": stale_dags,
            "concurrent_runs": concurrent_runs,
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
            complexity = [c for c in complexity if _get_dag_id(c) in allowed]

        context = {
            "dag_summary": _filter_dag_summary(dag_monitor.get_dag_status_summary(), allowed),
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
        cross_dag_deps = dep_monitor.get_cross_dag_dependencies()
        correlations = dep_monitor.get_failure_correlation(lookback_hours=24)

        # Filter cross-DAG dependencies and correlations
        if allowed is not None:
            if isinstance(cross_dag_deps, list):
                cross_dag_deps = [
                    d for d in cross_dag_deps
                    if _get_dag_id(d) in allowed
                ]
            if isinstance(correlations, list):
                correlations = [
                    c for c in correlations
                    if _get_dag_id(c) in allowed
                ]

        context = {
            "upstream_failures": upstream_failures,
            "cross_dag_deps": cross_dag_deps,
            "correlations": correlations,
            "show_hours_filter": False,
            **filter_ctx,
        }
        return self.render_template("watcher/dependencies.html", **context)
