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


@router.get(
    "/",
    response_model=Envelope,
    summary="System health check",
    response_description="HTTP 200 when healthy, HTTP 503 with Retry-After: 30 when degraded",
)
async def get_health(
    _auth: Optional[str] = Depends(require_auth),
):
    """Composite system health endpoint.

    Returns **HTTP 200** when `health_score ≥ 70` AND `import_error_count == 0`.
    Returns **HTTP 503** with `Retry-After: 30` otherwise.

    Fields:
    - `status` — `"healthy"` or `"degraded"`
    - `health_score` — 0–100 composite score
    - `import_error_count` — DAG files that failed to parse
    - `dag_health` — per-state task counts

    Cached 30 s.
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


@router.get(
    "/{dag_id}",
    summary="Per-DAG health detail",
    response_description="Last 10 failures and SLA misses for the specified DAG",
)
async def get_dag_health(
    dag_id: str,
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Return the last 10 failures and last 10 SLA misses for a specific DAG.

    Useful for a quick drill-down when the overview flags a DAG as problematic.
    Returns **403** if the caller's RBAC scope does not include `dag_id`.
    """
    check_dag_access(dag_id, allowed)

    failures = await asyncio.to_thread(get_failure_monitor().get_recent_failures, dag_id=dag_id, limit=10)
    sla_misses = await asyncio.to_thread(get_sla_monitor().get_recent_sla_misses, dag_id=dag_id, limit=10)

    return success_response(
        {
            "dag_id": dag_id,
            "recent_failures": [f.to_dict() for f in failures],
            "recent_sla_misses": [s.to_dict() for s in sla_misses],
            "failure_count": len(failures),
            "sla_miss_count": len(sla_misses),
        }
    )
