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


@router.get(
    "/import-errors",
    summary="DAG parse / import errors",
    response_description="Files that failed to parse with their error messages",
)
async def get_import_errors(
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Return DAG files that Airflow failed to parse.

    Each row includes `filename`, `error_message`, and `timestamp`.
    Parse errors prevent DAGs from being scheduled — fix them promptly.

    RBAC-filtered: only files whose path contains a permitted `dag_id` are returned.
    """
    cache = MetricsCache.get_instance()

    def _compute():
        return get_dag_health_monitor().get_dag_import_errors()

    errors = await asyncio.to_thread(cache.get_or_compute, "dags:import-errors", _compute)
    if allowed is not None:
        errors = [e for e in errors if any(dag_id in (e.get("filename") or "") for dag_id in allowed)]
    return success_response(
        {
            "errors": errors,
            "count": len(errors),
        }
    )


@router.get(
    "/status-summary",
    summary="Overall DAG health summary",
    response_description="Aggregate counts and composite health score (0–100)",
)
async def get_status_summary(
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Return an aggregate DAG health summary including a composite `health_score`.

    Fields include total active/paused/failed DAG counts and the `health_score`
    (0–100). A score below 70 sets `/health/` to HTTP 503.

    Aggregate counts are always global even when RBAC is active.
    """
    cache = MetricsCache.get_instance()

    def _compute():
        return get_dag_health_monitor().get_dag_status_summary()

    summary = await asyncio.to_thread(cache.get_or_compute, "dags:status-summary", _compute)
    return success_response(summary)


@router.get(
    "/complexity",
    summary="DAG complexity analysis",
    response_description="DAGs ranked by task count and structural complexity",
)
async def get_dag_complexity(
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Return complexity metrics per DAG, sorted by task count descending.

    Includes `task_count`, `branch_count`, and other structural signals.
    High-complexity DAGs are more likely to have cascading failures. RBAC-filtered.
    """
    cache = MetricsCache.get_instance()

    def _compute():
        return get_dag_health_monitor().get_dag_complexity_analysis()

    complexity = await asyncio.to_thread(cache.get_or_compute, "dags:complexity", _compute)
    complexity = filter_dags(complexity, allowed)

    return success_response(
        {
            "dags": complexity,
            "count": len(complexity),
        }
    )


@router.get(
    "/inactive",
    summary="Inactive DAGs",
    response_description="Active (unpaused) DAGs with no run in the specified number of days",
)
async def get_inactive_dags(
    days: int = Query(
        30, ge=1, le=365,
        description="Flag DAGs with no run in this many days.",
        example=30,
    ),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Return active (unpaused) DAGs that have not executed within `days` days.

    These may have broken schedules, misconfigured triggers, or be safe to retire.
    RBAC-filtered.
    """
    cache = MetricsCache.get_instance()
    cache_key = f"dags:inactive:days={days}"

    def _compute():
        return get_dag_health_monitor().get_inactive_dags(inactive_days=days)

    inactive = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    inactive = filter_dags(inactive, allowed)

    return success_response(
        {
            "inactive_dags": inactive,
            "count": len(inactive),
            "threshold_days": days,
        }
    )
