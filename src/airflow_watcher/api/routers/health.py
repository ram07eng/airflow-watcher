"""System and DAG health endpoints."""

import asyncio
from typing import Optional, Set

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from airflow_watcher.api.auth import require_auth
from airflow_watcher.api.envelope import success_response
from airflow_watcher.api.monitor_provider import get_dag_health_monitor, get_failure_monitor, get_sla_monitor
from airflow_watcher.api.rbac_dep import check_dag_access, get_allowed_dag_ids
from airflow_watcher.api.schemas import Envelope
from airflow_watcher.utils.cache import MetricsCache

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/", response_model=Envelope)
async def get_health(
    _auth: Optional[str] = Depends(require_auth),
):
    """System health endpoint.

    Returns HTTP 200 when ``health_score >= 70`` AND no import errors,
    otherwise HTTP 503.
    """
    cache = MetricsCache.get_instance()

    def _compute():
        dag_summary = get_dag_health_monitor().get_dag_status_summary()
        health_status = get_failure_monitor().get_dag_health_status()
        import_errors = get_dag_health_monitor().get_dag_import_errors()

        return {
            "dag_summary": dag_summary,
            "health_status": health_status,
            "import_errors_count": len(import_errors),
        }

    result = await asyncio.to_thread(cache.get_or_compute, "health", _compute, ttl=30)

    health_score = result["dag_summary"].get("health_score", 100)
    import_error_count = result["import_errors_count"]
    is_healthy = health_score >= 70 and import_error_count == 0

    payload = {
        "status": "healthy" if is_healthy else "degraded",
        "health_score": health_score,
        "summary": result["dag_summary"],
        "dag_health": result["health_status"].get("summary", {}),
        "import_error_count": import_error_count,
    }

    status_code = 200 if is_healthy else 503
    headers = {"Retry-After": "30"} if not is_healthy else {}
    return JSONResponse(content=success_response(payload), status_code=status_code, headers=headers)


@router.get("/{dag_id}")
async def get_dag_health(
    dag_id: str,
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get health status for a specific DAG."""
    check_dag_access(dag_id, allowed)

    failures = get_failure_monitor().get_recent_failures(dag_id=dag_id, limit=10)
    sla_misses = get_sla_monitor().get_recent_sla_misses(dag_id=dag_id, limit=10)

    return success_response(
        {
            "dag_id": dag_id,
            "recent_failures": [f.to_dict() for f in failures],
            "recent_sla_misses": [s.to_dict() for s in sla_misses],
            "failure_count": len(failures),
            "sla_miss_count": len(sla_misses),
        }
    )
