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


@router.get(
    "/lag",
    summary="Scheduling lag percentiles",
    response_description="p50/p90/p95 delay between scheduled time and first task start",
)
async def get_scheduling_lag(
    hours: int = Query(
        24, ge=1, le=8760,
        description="Lookback window in hours.",
        example=24,
    ),
    threshold_minutes: int = Query(
        10, ge=1, le=10080,
        description="DAG runs with lag exceeding this are listed in `delayed_dags`.",
        example=10,
    ),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Scheduling lag analysis — how long after `execution_date` does Airflow actually start work?

    Returns `p50_lag_minutes`, `p90_lag_minutes`, `p95_lag_minutes`, `avg_lag_minutes`.

    Aggregate percentiles are global. `delayed_dags` (runs exceeding `threshold_minutes`) is RBAC-filtered.
    """
    cache = MetricsCache.get_instance()
    cache_key = f"scheduling:lag:hours={hours}&threshold={threshold_minutes}"

    def _compute():
        return get_scheduling_monitor().get_scheduling_lag(
            lookback_hours=hours, lag_threshold_minutes=threshold_minutes
        )

    data = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    if allowed is not None and "delayed_dags" in data:
        data = {**data, "delayed_dags": [d for d in data["delayed_dags"] if d.get("dag_id") in allowed]}
    return success_response(data)


@router.get(
    "/queue",
    summary="Current task queue status",
    response_description="Queued and scheduled task counts with task lists",
)
async def get_queue_status(
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Return the current count and list of queued and scheduled task instances.

    `queued_tasks` and `scheduled_tasks` lists are RBAC-filtered.
    High queue depth relative to worker capacity indicates a bottleneck.
    """
    cache = MetricsCache.get_instance()

    def _compute():
        return get_scheduling_monitor().get_queued_tasks()

    data = await asyncio.to_thread(cache.get_or_compute, "scheduling:queue", _compute)
    if allowed is not None:
        for key in ("queued_tasks", "scheduled_tasks"):
            if key in data and isinstance(data[key], list):
                data = {**data, key: [t for t in data[key] if t.get("dag_id") in allowed]}
    return success_response(data)


@router.get(
    "/pools",
    summary="Worker pool utilisation",
    response_description="Per-pool slot usage: open, queued, running, deferred",
)
async def get_pool_utilization(
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Return slot utilisation for every Airflow worker pool.

    Pools are shared infrastructure — the list is not DAG-filtered.
    A pool with 0 open slots is a scheduling bottleneck.
    """
    cache = MetricsCache.get_instance()

    def _compute():
        return get_scheduling_monitor().get_pool_utilization()

    pools = await asyncio.to_thread(cache.get_or_compute, "scheduling:pools", _compute)
    return success_response(
        {
            "pools": pools,
            "count": len(pools),
        }
    )


@router.get(
    "/stale-dags",
    summary="Stale DAGs — overdue for a run",
    response_description="Active DAGs that haven't run within the expected interval",
)
async def get_stale_dags(
    expected_interval_hours: int = Query(
        24, ge=1, le=720,
        description="DAGs with no successful run in this many hours are flagged as stale.",
        example=24,
    ),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Return active (unpaused) DAGs that have not run within `expected_interval_hours`.

    Useful for detecting silently broken schedules — paused DAGs, misconfigured
    `schedule_interval`, or missed triggers. RBAC-filtered.
    """
    cache = MetricsCache.get_instance()
    cache_key = f"scheduling:stale:interval={expected_interval_hours}"

    def _compute():
        return get_scheduling_monitor().get_stale_dags(expected_interval_hours=expected_interval_hours)

    stale = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    stale = filter_dags(stale, allowed)

    return success_response(
        {
            "stale_dags": stale,
            "count": len(stale),
        }
    )


@router.get(
    "/concurrent",
    summary="DAGs with concurrent runs",
    response_description="DAGs running more than one instance simultaneously",
)
async def get_concurrent_runs(
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Return DAGs that currently have more than one run in `running` state.

    Concurrent runs can indicate missing `max_active_runs` limits or
    a backfill colliding with a scheduled run. RBAC-filtered.
    """
    cache = MetricsCache.get_instance()

    def _compute():
        return get_scheduling_monitor().get_concurrent_runs()

    data = await asyncio.to_thread(cache.get_or_compute, "scheduling:concurrent", _compute)
    if allowed is not None:
        filtered = dict(data)
        if isinstance(filtered.get("concurrent_dags"), list):
            filtered["concurrent_dags"] = [d for d in filtered["concurrent_dags"] if d.get("dag_id") in allowed]
            filtered["dags_with_concurrent_runs"] = len(filtered["concurrent_dags"])
    else:
        filtered = data
    return success_response(filtered)
