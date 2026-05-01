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


@router.get(
    "/",
    summary="List recent DAG failures",
    response_description="Paginated list of recent DAG run failures",
)
async def get_failures(
    dag_id: Optional[str] = Query(
        None,
        max_length=250,
        description="Filter to a specific DAG ID. Leave blank for all DAGs.",
        example="payment_etl",
    ),
    hours: int = Query(
        24,
        ge=1,
        le=8760,
        description="Lookback window in hours. Default 24 h, max 8 760 (1 year).",
        example=24,
    ),
    limit: int = Query(50, ge=1, le=500, description="Page size.", example=50),
    offset: int = Query(0, ge=0, description="Pagination offset.", example=0),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Return DAG run failure records within the lookback window.

    Each row represents one failed DAG run and includes `dag_id`, `run_id`,
    `execution_date`, `state`, and the task-level failure detail.

    Results are RBAC-filtered: callers only see DAGs they are permitted to access.

    **Tip:** start with `/failures/stats` for aggregate rates, then drill into
    specific DAGs using the `dag_id` filter here.
    """
    if dag_id:
        check_dag_access(dag_id, allowed)

    cache = MetricsCache.get_instance()
    cache_key = f"failures:{dag_id}:{hours}:{limit}:{offset}"

    def _compute():
        return get_failure_monitor().get_recent_failures(
            dag_id=dag_id, lookback_hours=hours, limit=limit, offset=offset
        )

    failures = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    data = [f.to_dict() for f in failures]
    data = filter_dags(data, allowed)

    return success_response(
        {
            "failures": data,
            "count": len(data),
            "filters": {"dag_id": dag_id, "hours": hours},
            "pagination": {"offset": offset, "limit": limit},
        }
    )


@router.get(
    "/stats",
    summary="Aggregate failure statistics",
    response_description="Failure rates, counts, and top offending DAGs",
)
async def get_failure_stats(
    hours: int = Query(
        24,
        ge=1,
        le=8760,
        description="Lookback window in hours.",
        example=24,
    ),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Aggregate failure statistics over the lookback window.

    Returns:
    - `total_runs` / `failed_runs` / `failure_rate`
    - `most_failing_dags` — top 10 DAGs by failure count (RBAC-filtered)

    **Good first stop for a dashboard summary view.**
    """
    cache = MetricsCache.get_instance()
    cache_key = f"failures:stats:{hours}"

    def _compute():
        return get_failure_monitor().get_failure_statistics(lookback_hours=hours)

    stats = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    # Filter per-DAG breakdowns
    if allowed is not None and "most_failing_dags" in stats:
        stats = {**stats, "most_failing_dags": [d for d in stats["most_failing_dags"] if d.get("dag_id") in allowed]}
    return success_response(stats)
