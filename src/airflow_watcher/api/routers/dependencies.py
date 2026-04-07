"""Dependency monitoring endpoints."""

import asyncio
import sys
from typing import Optional, Set

from fastapi import APIRouter, Depends, HTTPException, Query

from airflow_watcher.api.auth import require_auth
from airflow_watcher.api.envelope import success_response
from airflow_watcher.api.monitor_provider import get_dependency_monitor
from airflow_watcher.api.rbac_dep import check_dag_access, filter_dags, get_allowed_dag_ids
from airflow_watcher.utils.cache import MetricsCache

router = APIRouter(prefix="/dependencies", tags=["dependencies"])


def _is_standalone() -> bool:
    """True when running without a real Airflow install."""
    airflow_mod = sys.modules.get("airflow")
    return airflow_mod is not None and not hasattr(airflow_mod, "__version__")


@router.get("/upstream-failures")
async def get_upstream_failures(
    hours: int = Query(24, ge=1, le=8760),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get tasks in upstream_failed state."""
    cache = MetricsCache.get_instance()
    cache_key = f"dependencies:upstream:hours={hours}"

    def _compute():
        return get_dependency_monitor().get_upstream_failures(lookback_hours=hours)

    failures = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    failures = filter_dags(failures, allowed)

    return success_response(
        {
            "upstream_failures": failures,
            "count": len(failures),
        }
    )


@router.get("/cross-dag")
async def get_cross_dag_dependencies(
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get cross-DAG dependencies. RBAC-filtered by source/target dag_id.

    Returns 501 in standalone mode (requires DagBag which is unavailable).
    """
    if _is_standalone():
        raise HTTPException(
            status_code=501,
            detail="Cross-DAG dependency analysis requires a full Airflow install (DagBag).",
        )
    cache = MetricsCache.get_instance()

    def _compute():
        return get_dependency_monitor().get_cross_dag_dependencies()

    deps = await asyncio.to_thread(cache.get_or_compute, "dependencies:cross-dag", _compute)
    if allowed is not None:
        deps = [
            d
            for d in deps
            if isinstance(d, dict)
            and d.get("dag_id", d.get("source_dag_id")) in allowed
            and d.get("target_dag_id", d.get("dag_id")) in allowed
        ]
    return success_response(
        {
            "dependencies": deps,
            "count": len(deps),
        }
    )


@router.get("/correlations")
async def get_failure_correlations(
    hours: int = Query(24, ge=1, le=8760),
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get failure correlations between DAGs. RBAC-filtered."""
    cache = MetricsCache.get_instance()
    cache_key = f"dependencies:correlations:hours={hours}"

    def _compute():
        return get_dependency_monitor().get_failure_correlation(lookback_hours=hours)

    data = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)
    if allowed is not None and isinstance(data, dict):
        for key in ("correlations", "correlated_failures"):
            if key in data and isinstance(data[key], list):
                data = {
                    **data,
                    key: [
                        c
                        for c in data[key]
                        if isinstance(c, dict)
                        and (c.get("dag_id") in allowed or c.get("dag_id_a") in allowed)
                        and (c.get("dag_id") in allowed or c.get("dag_id_b") in allowed)
                    ],
                }
    return success_response(data)


@router.get("/impact/{dag_id}/{task_id}")
async def get_task_impact(
    dag_id: str,
    task_id: str,
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get downstream impact of a task failure.

    Returns 501 in standalone mode (requires DagBag).
    """
    if _is_standalone():
        raise HTTPException(
            status_code=501,
            detail="Cascading failure impact analysis requires a full Airflow install (DagBag).",
        )
    if len(dag_id) > 250 or len(task_id) > 250:
        raise HTTPException(status_code=400, detail="Invalid dag_id or task_id length")

    check_dag_access(dag_id, allowed)

    cache = MetricsCache.get_instance()
    cache_key = f"dependencies:impact:{dag_id}:{task_id}"

    def _compute():
        return get_dependency_monitor().get_cascading_failure_impact(dag_id=dag_id, task_id=task_id)

    data = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute)

    # Filter downstream references if RBAC is active to prevent cross-DAG leakage.
    if allowed is not None and isinstance(data, dict):
        for key in ("downstream", "downstream_tasks", "impacted_tasks"):
            if key in data and isinstance(data[key], list):
                data[key] = [item for item in data[key] if isinstance(item, dict) and item.get("dag_id") in allowed]

    return success_response(data)
