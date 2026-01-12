"""API endpoints for Airflow Watcher."""

from flask import Blueprint, jsonify, request
from datetime import datetime

from airflow_watcher.monitors.dag_failure_monitor import DAGFailureMonitor
from airflow_watcher.monitors.sla_monitor import SLAMonitor


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
        "timestamp": datetime.utcnow().isoformat(),
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
        "timestamp": datetime.utcnow().isoformat(),
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
        "timestamp": datetime.utcnow().isoformat(),
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
        "timestamp": datetime.utcnow().isoformat(),
    })


@watcher_api_blueprint.route("/health", methods=["GET"])
def get_health():
    """Get DAG health status via API."""
    failure_monitor = DAGFailureMonitor()
    
    health_status = failure_monitor.get_dag_health_status()
    
    return jsonify({
        "status": "success",
        "data": health_status,
        "timestamp": datetime.utcnow().isoformat(),
    })


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
        "timestamp": datetime.utcnow().isoformat(),
    })
