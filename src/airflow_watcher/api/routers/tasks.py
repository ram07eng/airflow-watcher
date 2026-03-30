"""Task health monitoring endpoints."""

import asyncio
from typing import Optional, Set

from fastapi import APIRouter, Depends, Query

from airflow_watcher.api.auth import require_auth
from airflow_watcher.api.envelope import success_response
from airflow_watcher.api.monitor_provider import get_task_monitor
from airflow_watcher.api.rbac_dep import filter_dags, get_allowed_dag_ids
from airflow_watcher.utils.cache import MetricsCache

router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.get("/long-running")
async def get_long_running_tasks(
    threshold_minutes: int = Query(60, ge=1, le=10080),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get tasks running longer than the threshold, sorted by duration descending."""
    cache = MetricsCache.get_instance()
    cache_key = f"tasks:long-running:threshold={threshold_minutes}"

    def _compute():
        return get_task_monitor().get_long_running_tasks(threshold_minutes=threshold_minutes)

    tasks = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    tasks = filter_dags(tasks, allowed)

    return success_response({
        "tasks": tasks,
        "count": len(tasks),
        "threshold_minutes": threshold_minutes,
    })


@router.get("/retries")
async def get_retry_heavy_tasks(
    hours: int = Query(24, ge=1, le=8760),
    min_retries: int = Query(2, ge=1, le=100),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get tasks with excessive retries."""
    cache = MetricsCache.get_instance()
    cache_key = f"tasks:retries:hours={hours}&min={min_retries}"

    def _compute():
        return get_task_monitor().get_retry_heavy_tasks(lookback_hours=hours, min_retries=min_retries)

    tasks = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    tasks = filter_dags(tasks, allowed)

    return success_response({
        "tasks": tasks,
        "count": len(tasks),
    })


@router.get("/zombies")
async def get_zombie_tasks(
    threshold_minutes: int = Query(120, ge=1, le=10080),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get potential zombie tasks."""
    cache = MetricsCache.get_instance()
    cache_key = f"tasks:zombies:threshold={threshold_minutes}"

    def _compute():
        return get_task_monitor().get_zombie_tasks(zombie_threshold_minutes=threshold_minutes)

    tasks = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    tasks = filter_dags(tasks, allowed)

    return success_response({
        "zombies": tasks,
        "count": len(tasks),
        "threshold_minutes": threshold_minutes,
    })


@router.get("/failure-patterns")
async def get_failure_patterns(
    hours: int = Query(168, ge=1, le=8760),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get task failure pattern analysis.

    Note: Per-task breakdowns are filtered by RBAC when enabled.
    """
    cache = MetricsCache.get_instance()
    cache_key = f"tasks:failure-patterns:hours={hours}"

    def _compute():
        return get_task_monitor().get_task_failure_patterns(lookback_hours=hours)

    patterns = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    if allowed is not None:
        filtered = {**patterns}
        for key in ("top_failing_tasks", "flaky_tasks"):
            if key in filtered:
                filtered[key] = [
                    t for t in filtered[key] if t.get("dag_id") in allowed
                ]
        patterns = filtered
    return success_response(patterns)
