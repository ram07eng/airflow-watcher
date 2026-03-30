"""Combined monitoring overview endpoint."""

import asyncio
import hashlib
from typing import Optional, Set

from fastapi import APIRouter, Depends

from airflow_watcher.api.auth import require_auth
from airflow_watcher.api.envelope import success_response
from airflow_watcher.api.monitor_provider import (
    get_dag_health_monitor,
    get_failure_monitor,
    get_scheduling_monitor,
    get_sla_monitor,
    get_task_monitor,
)
from airflow_watcher.api.rbac_dep import get_allowed_dag_ids
from airflow_watcher.api.schemas import Envelope
from airflow_watcher.utils.cache import MetricsCache

router = APIRouter(prefix="/overview", tags=["overview"])


def _allowed_hash(allowed: Optional[Set[str]]) -> str:
    """Return a short hash of the allowed DAG set for cache keying."""
    if allowed is None:
        return "all"
    return hashlib.sha256(",".join(sorted(allowed)).encode()).hexdigest()[:12]


@router.get("/", response_model=Envelope)
async def get_overview(
    allowed: Optional[Set[str]] = Depends(get_allowed_dag_ids),
    _auth: Optional[str] = Depends(require_auth),
):
    """Get comprehensive monitoring snapshot (cached 60s).

    Cache is keyed per-user (allowed DAG set) so restricted users do not
    see cached results from a broader scope.
    """
    cache = MetricsCache.get_instance()
    cache_key = f"overview:{_allowed_hash(allowed)}"

    def _compute():
        return {
            "failure_stats": get_failure_monitor().get_failure_statistics(lookback_hours=24),
            "sla_stats": get_sla_monitor().get_sla_statistics(lookback_hours=24),
            "long_running_tasks": len(get_task_monitor().get_long_running_tasks(threshold_minutes=60)),
            "zombie_count": len(get_task_monitor().get_zombie_tasks()),
            "queue_status": get_scheduling_monitor().get_queued_tasks(),
            "dag_summary": get_dag_health_monitor().get_dag_status_summary(),
            "import_errors": len(get_dag_health_monitor().get_dag_import_errors()),
        }

    data = await asyncio.to_thread(cache.get_or_compute, cache_key, _compute, ttl=60)

    if allowed is not None:
        data = {**data}
        # Filter per-DAG breakdowns inside sub-dicts
        for stats_key in ("failure_stats", "sla_stats"):
            if isinstance(data.get(stats_key), dict):
                sub = data[stats_key]
                for list_key in ("most_failing_dags", "top_dags_with_misses", "top_tasks_with_misses"):
                    if list_key in sub and isinstance(sub[list_key], list):
                        data[stats_key] = {**sub, list_key: [
                            d for d in sub[list_key] if d.get("dag_id") in allowed
                        ]}
                        sub = data[stats_key]
        # Filter queue task lists
        qs = data.get("queue_status")
        if isinstance(qs, dict):
            for key in ("queued_tasks", "scheduled_tasks"):
                if key in qs and isinstance(qs[key], list):
                    data["queue_status"] = {**qs, key: [
                        t for t in qs[key] if t.get("dag_id") in allowed
                    ]}
                    qs = data["queue_status"]

    return success_response(data)
