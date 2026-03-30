"""Cache management endpoint."""

from typing import Optional

from fastapi import APIRouter, Depends

from airflow_watcher.api.auth import require_auth
from airflow_watcher.api.envelope import success_response
from airflow_watcher.utils.cache import MetricsCache

router = APIRouter(prefix="/cache", tags=["cache"])


@router.post("/invalidate")
def invalidate_cache(
    _auth: Optional[str] = Depends(require_auth),
):
    """Clear all cached entries."""
    cache = MetricsCache.get_instance()
    cache.clear()

    return success_response({
        "cleared": True,
        "message": "All cache entries cleared.",
    })
