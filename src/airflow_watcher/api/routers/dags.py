"""DAG health monitoring endpoints."""

import asyncio
from typing import Optional, Set

from fastapi import APIRouter, Depends, Query

from airflow_watcher.api.auth import require_auth
from airflow_watcher.api.envelope import success_response
from airflow_watcher.api.monitor_provider import get_dag_health_monitor
from airflow_watcher.api.rbac_dep import filter_dags, get_allowed_dag_ids
from airflow_watcher.utils.cache import MetricsCache

router = APIRouter(prefix="/dags", tags=["dags"])


@router.get("/import-errors")
async def get_import_errors(
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get DAG import/parse errors. RBAC-filtered by filename containing dag_id."""
    cache = MetricsCache.get_instance()

    def _compute():
        return get_dag_health_monitor().get_dag_import_errors()

    errors = await asyncio.to_thread(cache.get_or_compute, "dags:import-errors", _compute)
    if allowed is not None:
        errors = [
            e for e in errors
            if any(dag_id in (e.get("filename") or "") for dag_id in allowed)
        ]
    return success_response({
        "errors": errors,
        "count": len(errors),
    })


@router.get("/status-summary")
async def get_status_summary(
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get overall DAG status summary including health score.

    When RBAC is active, aggregate counts are still global (they do not
    reveal per-DAG information).  The health_score is also a global metric.
    """
    cache = MetricsCache.get_instance()

    def _compute():
        return get_dag_health_monitor().get_dag_status_summary()

    summary = await asyncio.to_thread(cache.get_or_compute, "dags:status-summary", _compute)
    return success_response(summary)


@router.get("/complexity")
async def get_dag_complexity(
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get DAG complexity analysis, sorted by task count descending."""
    cache = MetricsCache.get_instance()

    def _compute():
        return get_dag_health_monitor().get_dag_complexity_analysis()

    complexity = await asyncio.to_thread(cache.get_or_compute, "dags:complexity", _compute)
    complexity = filter_dags(complexity, allowed)

    return success_response({
        "dags": complexity,
        "count": len(complexity),
    })


@router.get("/inactive")
async def get_inactive_dags(
    days: int = Query(30, ge=1, le=365),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get active DAGs that haven't run within the specified days."""
    cache = MetricsCache.get_instance()
    cache_key = f"dags:inactive:days={days}"

    def _compute():
        return get_dag_health_monitor().get_inactive_dags(inactive_days=days)

    inactive = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    inactive = filter_dags(inactive, allowed)

    return success_response({
        "inactive_dags": inactive,
        "count": len(inactive),
        "threshold_days": days,
    })
