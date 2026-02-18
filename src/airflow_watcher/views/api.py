"""API endpoints for Airflow Watcher."""

from airflow.utils import timezone
from flask import Blueprint, jsonify, request

from airflow_watcher.monitors.dag_failure_monitor import DAGFailureMonitor
from airflow_watcher.monitors.dag_health_monitor import DAGHealthMonitor
from airflow_watcher.monitors.dependency_monitor import DependencyMonitor
from airflow_watcher.monitors.scheduling_monitor import SchedulingMonitor
from airflow_watcher.monitors.sla_monitor import SLAMonitor
from airflow_watcher.monitors.task_health_monitor import TaskHealthMonitor

watcher_api_blueprint = Blueprint("watcher_api", __name__, url_prefix="/api/watcher")


@watcher_api_blueprint.route("/failures", methods=["GET"])
def get_failures():
    """Get recent DAG failures via API."""
    failure_monitor = DAGFailureMonitor()

    dag_id = request.args.get("dag_id")
    hours = int(request.args.get("hours", 24))
    limit = int(request.args.get("limit", 50))

    failures = failure_monitor.get_recent_failures(
        dag_id=dag_id,
        lookback_hours=hours,
        limit=limit
    )

    return jsonify({
        "status": "success",
        "data": {
            "failures": [f.to_dict() for f in failures],
            "count": len(failures),
            "filters": {"dag_id": dag_id, "hours": hours},
        },
        "timestamp": timezone.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/failures/stats", methods=["GET"])
def get_failure_stats():
    """Get failure statistics via API."""
    failure_monitor = DAGFailureMonitor()

    hours = int(request.args.get("hours", 24))
    stats = failure_monitor.get_failure_statistics(lookback_hours=hours)

    return jsonify({
        "status": "success",
        "data": stats,
        "timestamp": timezone.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/sla/misses", methods=["GET"])
def get_sla_misses():
    """Get SLA misses via API."""
    sla_monitor = SLAMonitor()

    dag_id = request.args.get("dag_id")
    hours = int(request.args.get("hours", 24))
    limit = int(request.args.get("limit", 50))

    sla_misses = sla_monitor.get_recent_sla_misses(
        dag_id=dag_id,
        lookback_hours=hours,
        limit=limit
    )

    return jsonify({
        "status": "success",
        "data": {
            "sla_misses": [s.to_dict() for s in sla_misses],
            "count": len(sla_misses),
            "filters": {"dag_id": dag_id, "hours": hours},
        },
        "timestamp": timezone.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/sla/stats", methods=["GET"])
def get_sla_stats():
    """Get SLA statistics via API."""
    sla_monitor = SLAMonitor()

    hours = int(request.args.get("hours", 24))
    stats = sla_monitor.get_sla_statistics(lookback_hours=hours)

    return jsonify({
        "status": "success",
        "data": stats,
        "timestamp": timezone.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/health", methods=["GET"])
def get_health():
    """Health check endpoint - returns overall system health summary.

    Suitable for external monitoring tools (e.g. Datadog, Prometheus alertmanager,
    uptime checkers) to poll. Returns HTTP 200 when healthy, 503 when degraded.
    """
    dag_monitor = DAGHealthMonitor()
    failure_monitor = DAGFailureMonitor()

    try:
        dag_summary = dag_monitor.get_dag_status_summary()
        health_status = failure_monitor.get_dag_health_status()
        import_errors = dag_monitor.get_dag_import_errors()

        health_score = dag_summary.get("health_score", 100)
        is_healthy = health_score >= 70 and len(import_errors) == 0

        payload = {
            "status": "healthy" if is_healthy else "degraded",
            "health_score": health_score,
            "summary": dag_summary,
            "dag_health": health_status.get("summary", {}),
            "import_error_count": len(import_errors),
            "timestamp": timezone.utcnow().isoformat(),
        }

        http_status = 200 if is_healthy else 503
        return jsonify({"status": "success", "data": payload}), http_status

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": timezone.utcnow().isoformat(),
        }), 503


@watcher_api_blueprint.route("/health/<dag_id>", methods=["GET"])
def get_dag_health(dag_id: str):
    """Get health status for a specific DAG."""
    failure_monitor = DAGFailureMonitor()
    sla_monitor = SLAMonitor()

    dag_failures = failure_monitor.get_recent_failures(dag_id=dag_id, limit=10)
    dag_sla_misses = sla_monitor.get_recent_sla_misses(dag_id=dag_id, limit=10)

    return jsonify({
        "status": "success",
        "data": {
            "dag_id": dag_id,
            "recent_failures": [f.to_dict() for f in dag_failures],
            "recent_sla_misses": [s.to_dict() for s in dag_sla_misses],
            "failure_count": len(dag_failures),
            "sla_miss_count": len(dag_sla_misses),
        },
        "timestamp": timezone.utcnow().isoformat(),
    })


# ============== Task Health Endpoints ==============

@watcher_api_blueprint.route("/tasks/long-running", methods=["GET"])
def get_long_running_tasks():
    """Get tasks running longer than expected."""
    monitor = TaskHealthMonitor()
    threshold = int(request.args.get("threshold_minutes", 60))

    tasks = monitor.get_long_running_tasks(threshold_minutes=threshold)

    return jsonify({
        "status": "success",
        "data": {
            "tasks": tasks,
            "count": len(tasks),
            "threshold_minutes": threshold,
        },
        "timestamp": timezone.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/tasks/retries", methods=["GET"])
def get_retry_heavy_tasks():
    """Get tasks with excessive retries."""
    monitor = TaskHealthMonitor()
    hours = int(request.args.get("hours", 24))
    min_retries = int(request.args.get("min_retries", 2))

    tasks = monitor.get_retry_heavy_tasks(lookback_hours=hours, min_retries=min_retries)

    return jsonify({
        "status": "success",
        "data": {
            "tasks": tasks,
            "count": len(tasks),
        },
        "timestamp": timezone.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/tasks/zombies", methods=["GET"])
def get_zombie_tasks():
    """Get potential zombie tasks."""
    monitor = TaskHealthMonitor()
    threshold = int(request.args.get("threshold_minutes", 120))

    zombies = monitor.get_zombie_tasks(zombie_threshold_minutes=threshold)

    return jsonify({
        "status": "success",
        "data": {
            "zombies": zombies,
            "count": len(zombies),
            "threshold_minutes": threshold,
        },
        "timestamp": timezone.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/tasks/failure-patterns", methods=["GET"])
def get_task_failure_patterns():
    """Get task failure pattern analysis."""
    monitor = TaskHealthMonitor()
    hours = int(request.args.get("hours", 168))  # 7 days default

    patterns = monitor.get_task_failure_patterns(lookback_hours=hours)

    return jsonify({
        "status": "success",
        "data": patterns,
        "timestamp": timezone.utcnow().isoformat(),
    })


# ============== Scheduling Endpoints ==============

@watcher_api_blueprint.route("/scheduling/lag", methods=["GET"])
def get_scheduling_lag():
    """Get scheduling lag analysis."""
    monitor = SchedulingMonitor()
    hours = int(request.args.get("hours", 24))
    threshold = int(request.args.get("threshold_minutes", 10))

    lag_data = monitor.get_scheduling_lag(
        lookback_hours=hours,
        lag_threshold_minutes=threshold,
    )

    return jsonify({
        "status": "success",
        "data": lag_data,
        "timestamp": timezone.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/scheduling/queue", methods=["GET"])
def get_queue_status():
    """Get current queue status."""
    monitor = SchedulingMonitor()

    queue_data = monitor.get_queued_tasks()

    return jsonify({
        "status": "success",
        "data": queue_data,
        "timestamp": timezone.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/scheduling/pools", methods=["GET"])
def get_pool_utilization():
    """Get pool utilization stats."""
    monitor = SchedulingMonitor()

    pools = monitor.get_pool_utilization()

    return jsonify({
        "status": "success",
        "data": {
            "pools": pools,
            "count": len(pools),
        },
        "timestamp": timezone.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/scheduling/stale-dags", methods=["GET"])
def get_stale_dags():
    """Get DAGs that haven't run when expected."""
    monitor = SchedulingMonitor()
    hours = int(request.args.get("expected_interval_hours", 24))

    stale = monitor.get_stale_dags(expected_interval_hours=hours)

    return jsonify({
        "status": "success",
        "data": {
            "stale_dags": stale,
            "count": len(stale),
        },
        "timestamp": timezone.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/scheduling/concurrent", methods=["GET"])
def get_concurrent_runs():
    """Get DAGs with multiple concurrent runs."""
    monitor = SchedulingMonitor()

    concurrent = monitor.get_concurrent_runs()

    return jsonify({
        "status": "success",
        "data": concurrent,
        "timestamp": timezone.utcnow().isoformat(),
    })


# ============== DAG Health Endpoints ==============

@watcher_api_blueprint.route("/dags/import-errors", methods=["GET"])
def get_import_errors():
    """Get DAG import/parse errors."""
    monitor = DAGHealthMonitor()

    errors = monitor.get_dag_import_errors()

    return jsonify({
        "status": "success",
        "data": {
            "errors": errors,
            "count": len(errors),
        },
        "timestamp": timezone.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/dags/status-summary", methods=["GET"])
def get_dag_status_summary():
    """Get overall DAG status summary."""
    monitor = DAGHealthMonitor()

    summary = monitor.get_dag_status_summary()

    return jsonify({
        "status": "success",
        "data": summary,
        "timestamp": timezone.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/dags/complexity", methods=["GET"])
def get_dag_complexity():
    """Get DAG complexity analysis."""
    monitor = DAGHealthMonitor()

    complexity = monitor.get_dag_complexity_analysis()

    return jsonify({
        "status": "success",
        "data": {
            "dags": complexity,
            "count": len(complexity),
        },
        "timestamp": timezone.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/dags/inactive", methods=["GET"])
def get_inactive_dags():
    """Get inactive DAGs."""
    monitor = DAGHealthMonitor()
    days = int(request.args.get("days", 30))

    inactive = monitor.get_inactive_dags(inactive_days=days)

    return jsonify({
        "status": "success",
        "data": {
            "inactive_dags": inactive,
            "count": len(inactive),
            "threshold_days": days,
        },
        "timestamp": timezone.utcnow().isoformat(),
    })


# ============== Dependency Endpoints ==============

@watcher_api_blueprint.route("/dependencies/upstream-failures", methods=["GET"])
def get_upstream_failures():
    """Get tasks in upstream_failed state."""
    monitor = DependencyMonitor()
    hours = int(request.args.get("hours", 24))

    failures = monitor.get_upstream_failures(lookback_hours=hours)

    return jsonify({
        "status": "success",
        "data": {
            "upstream_failures": failures,
            "count": len(failures),
        },
        "timestamp": timezone.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/dependencies/cross-dag", methods=["GET"])
def get_cross_dag_dependencies():
    """Get cross-DAG dependencies."""
    monitor = DependencyMonitor()

    dependencies = monitor.get_cross_dag_dependencies()

    return jsonify({
        "status": "success",
        "data": {
            "dependencies": dependencies,
            "count": len(dependencies),
        },
        "timestamp": timezone.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/dependencies/correlations", methods=["GET"])
def get_failure_correlations():
    """Get failure correlations between DAGs."""
    monitor = DependencyMonitor()
    hours = int(request.args.get("hours", 24))

    correlations = monitor.get_failure_correlation(lookback_hours=hours)

    return jsonify({
        "status": "success",
        "data": correlations,
        "timestamp": timezone.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/dependencies/impact/<dag_id>/<task_id>", methods=["GET"])
def get_task_failure_impact(dag_id: str, task_id: str):
    """Get downstream impact of a task failure."""
    monitor = DependencyMonitor()

    impact = monitor.get_cascading_failure_impact(dag_id=dag_id, task_id=task_id)

    return jsonify({
        "status": "success",
        "data": impact,
        "timestamp": timezone.utcnow().isoformat(),
    })


# ============== Combined Dashboard Endpoint ==============

@watcher_api_blueprint.route("/overview", methods=["GET"])
def get_full_overview():
    """Get comprehensive monitoring overview."""
    failure_monitor = DAGFailureMonitor()
    sla_monitor = SLAMonitor()
    task_monitor = TaskHealthMonitor()
    scheduling_monitor = SchedulingMonitor()
    dag_monitor = DAGHealthMonitor()

    return jsonify({
        "status": "success",
        "data": {
            "failure_stats": failure_monitor.get_failure_statistics(lookback_hours=24),
            "sla_stats": sla_monitor.get_sla_statistics(lookback_hours=24),
            "long_running_tasks": len(task_monitor.get_long_running_tasks(threshold_minutes=60)),
            "zombie_count": len(task_monitor.get_zombie_tasks()),
            "queue_status": scheduling_monitor.get_queued_tasks(),
            "dag_summary": dag_monitor.get_dag_status_summary(),
            "import_errors": len(dag_monitor.get_dag_import_errors()),
        },
        "timestamp": timezone.utcnow().isoformat(),
    })
