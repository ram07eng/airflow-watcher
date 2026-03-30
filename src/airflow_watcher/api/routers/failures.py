"""DAG failure monitoring endpoints."""

import asyncio
from typing import Optional, Set

from fastapi import APIRouter, Depends, Query

from airflow_watcher.api.auth import require_auth
from airflow_watcher.api.envelope import success_response
from airflow_watcher.api.monitor_provider import get_failure_monitor
from airflow_watcher.api.rbac_dep import check_dag_access, filter_dags, get_allowed_dag_ids
from airflow_watcher.utils.cache import MetricsCache

router = APIRouter(prefix="/failures", tags=["failures"])


@router.get("/")
async def get_failures(
    dag_id: Optional[str] = Query(None, max_length=250),
    hours: int = Query(24, ge=1, le=8760),
    limit: int = Query(50, ge=1, le=500),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get recent DAG failures."""
    if dag_id:
        check_dag_access(dag_id, allowed)

    cache = MetricsCache.get_instance()
    cache_key = f"failures:{dag_id}:{hours}:{limit}"

    def _compute():
        return get_failure_monitor().get_recent_failures(dag_id=dag_id, lookback_hours=hours, limit=limit)

    failures = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    data = [f.to_dict() for f in failures]
    data = filter_dags(data, allowed)

    return success_response({
        "failures": data,
        "count": len(data),
        "filters": {"dag_id": dag_id, "hours": hours},
    })


@router.get("/stats")
async def get_failure_stats(
    hours: int = Query(24, ge=1, le=8760),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get failure statistics.

    Note: Returns global aggregate metrics. The ``most_failing_dags`` list
    is filtered by the caller's RBAC permissions when RBAC is enabled.
    """
    cache = MetricsCache.get_instance()
    cache_key = f"failures:stats:{hours}"

    def _compute():
        return get_failure_monitor().get_failure_statistics(lookback_hours=hours)

    stats = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    # Filter per-DAG breakdowns
    if allowed is not None and "most_failing_dags" in stats:
        stats = {**stats, "most_failing_dags": [
            d for d in stats["most_failing_dags"] if d.get("dag_id") in allowed
        ]}
    return success_response(stats)
