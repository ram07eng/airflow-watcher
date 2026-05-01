"""Cache management endpoint."""

from typing import Optional

from fastapi import APIRouter, Depends

from airflow_watcher.api.auth import require_auth
from airflow_watcher.api.envelope import success_response
from airflow_watcher.utils.cache import MetricsCache

router = APIRouter(prefix="/cache", tags=["cache"])


@router.post(
    "/invalidate",
    summary="Flush the in-memory response cache",
    response_description="Confirmation that all cache entries were cleared",
)
def invalidate_cache(
    _auth: Optional[str] = Depends(require_auth),
):
    """Clear all in-memory cached responses immediately.

    The next request to any cached endpoint will re-query the database.
    Use after a bulk backfill, a deployment, or when you suspect stale data.
    """
    cache = MetricsCache.get_instance()
    cache.clear()

    return success_response(
        {
            "cleared": True,
            "message": "All cache entries cleared.",
        }
    )
