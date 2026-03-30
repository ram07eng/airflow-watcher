"""Scheduling monitoring endpoints."""

import asyncio
from typing import Optional, Set

from fastapi import APIRouter, Depends, Query

from airflow_watcher.api.auth import require_auth
from airflow_watcher.api.envelope import success_response
from airflow_watcher.api.monitor_provider import get_scheduling_monitor
from airflow_watcher.api.rbac_dep import filter_dags, get_allowed_dag_ids
from airflow_watcher.utils.cache import MetricsCache

router = APIRouter(prefix="/scheduling", tags=["scheduling"])


@router.get("/lag")
async def get_scheduling_lag(
    hours: int = Query(24, ge=1, le=8760),
    threshold_minutes: int = Query(10, ge=1, le=10080),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get scheduling lag statistics and delayed DAGs.

    Note: Aggregate counts are global. The delayed_dags list is RBAC-filtered.
    """
    cache = MetricsCache.get_instance()
    cache_key = f"scheduling:lag:hours={hours}&threshold={threshold_minutes}"

    def _compute():
        return get_scheduling_monitor().get_scheduling_lag(
            lookback_hours=hours, lag_threshold_minutes=threshold_minutes
        )

    data = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    if allowed is not None and "delayed_dags" in data:
        data = {**data, "delayed_dags": [
            d for d in data["delayed_dags"] if d.get("dag_id") in allowed
        ]}
    return success_response(data)


@router.get("/queue")
async def get_queue_status(
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get current queue status. Task lists are RBAC-filtered."""
    cache = MetricsCache.get_instance()

    def _compute():
        return get_scheduling_monitor().get_queued_tasks()

    data = await asyncio.to_thread(cache.get_or_compute, "scheduling:queue", _compute)
    if allowed is not None:
        for key in ("queued_tasks", "scheduled_tasks"):
            if key in data and isinstance(data[key], list):
                data = {**data, key: [t for t in data[key] if t.get("dag_id") in allowed]}
    return success_response(data)


@router.get("/pools")
async def get_pool_utilization(
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get pool utilization stats.

    Note: Pools are shared infrastructure; the list is not DAG-filtered
    but requires authentication. The allowed parameter is accepted but
    pools are global resources not tied to individual DAGs.
    """
    cache = MetricsCache.get_instance()

    def _compute():
        return get_scheduling_monitor().get_pool_utilization()

    pools = await asyncio.to_thread(cache.get_or_compute, "scheduling:pools", _compute)
    return success_response({
        "pools": pools,
        "count": len(pools),
    })


@router.get("/stale-dags")
async def get_stale_dags(
    expected_interval_hours: int = Query(24, ge=1, le=720),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get DAGs that haven't run when expected."""
    cache = MetricsCache.get_instance()
    cache_key = f"scheduling:stale:interval={expected_interval_hours}"

    def _compute():
        return get_scheduling_monitor().get_stale_dags(expected_interval_hours=expected_interval_hours)

    stale = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    stale = filter_dags(stale, allowed)

    return success_response({
        "stale_dags": stale,
        "count": len(stale),
    })


@router.get("/concurrent")
async def get_concurrent_runs(
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get DAGs with multiple concurrent runs. List is RBAC-filtered."""
    cache = MetricsCache.get_instance()

    def _compute():
        return get_scheduling_monitor().get_concurrent_runs()

    data = await asyncio.to_thread(cache.get_or_compute, "scheduling:concurrent", _compute)
    if allowed is not None:
        filtered = dict(data)
        if isinstance(filtered.get("concurrent_dags"), list):
            filtered["concurrent_dags"] = [
                d for d in filtered["concurrent_dags"] if d.get("dag_id") in allowed
            ]
            filtered["dags_with_concurrent_runs"] = len(filtered["concurrent_dags"])
    else:
        filtered = data
    return success_response(filtered)
