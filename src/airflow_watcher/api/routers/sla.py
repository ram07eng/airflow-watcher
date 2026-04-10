"""SLA monitoring endpoints."""

import asyncio
from typing import Optional, Set

from fastapi import APIRouter, Depends, Query

from airflow_watcher.api.auth import require_auth
from airflow_watcher.api.envelope import success_response
from airflow_watcher.api.monitor_provider import get_sla_monitor
from airflow_watcher.api.rbac_dep import check_dag_access, filter_dags, get_allowed_dag_ids
from airflow_watcher.utils.cache import MetricsCache

router = APIRouter(prefix="/sla", tags=["sla"])


@router.get("/misses")
async def get_sla_misses(
    dag_id: Optional[str] = Query(None, max_length=250),
    hours: int = Query(24, ge=1, le=8760),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get SLA miss events."""
    if dag_id:
        check_dag_access(dag_id, allowed)

    cache = MetricsCache.get_instance()
    cache_key = f"sla:misses:{dag_id}:{hours}:{limit}:{offset}"

    def _compute():
        return get_sla_monitor().get_recent_sla_misses(dag_id=dag_id, lookback_hours=hours, limit=limit, offset=offset)

    misses = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    data = [m.to_dict() for m in misses]
    data = filter_dags(data, allowed)

    return success_response(
        {
            "sla_misses": data,
            "count": len(data),
            "filters": {"dag_id": dag_id, "hours": hours},
            "pagination": {"offset": offset, "limit": limit},
        }
    )


@router.get("/stats")
async def get_sla_stats(
    hours: int = Query(24, ge=1, le=8760),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get SLA statistics.

    Note: Aggregate counts are global. Per-DAG breakdowns are filtered by RBAC.
    """
    cache = MetricsCache.get_instance()
    cache_key = f"sla:stats:{hours}"

    def _compute():
        return get_sla_monitor().get_sla_statistics(lookback_hours=hours)

    stats = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    if allowed is not None:
        for key in ("top_dags_with_misses", "top_tasks_with_misses"):
            if key in stats:
                stats = {**stats, key: [d for d in stats[key] if d.get("dag_id") in allowed]}
    return success_response(stats)
