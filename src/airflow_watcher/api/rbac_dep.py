"""RBAC FastAPI dependency for the Airflow Watcher API.

Maps an authenticated API key to a set of allowed DAG IDs using the
``rbac_key_dag_mapping`` configuration.  When RBAC is disabled, every
caller sees all DAGs.
"""

import logging
from typing import Any, Dict, List, Optional, Set

from fastapi import Depends, HTTPException

from airflow_watcher.api.auth import require_auth
from airflow_watcher.api.envelope import error_response

logger = logging.getLogger(__name__)

# Module-level state set at startup.
_rbac_enabled: bool = False
_key_dag_mapping: Dict[str, List[str]] = {}
_rbac_fail_open: bool = False


def configure_rbac(
    enabled: bool,
    key_dag_mapping: Dict[str, List[str]],
    fail_open: bool = False,
) -> None:
    """Configure RBAC settings.  Called once at startup."""
    global _rbac_enabled, _key_dag_mapping, _rbac_fail_open
    _rbac_enabled = enabled
    _key_dag_mapping = dict(key_dag_mapping)
    _rbac_fail_open = fail_open


async def get_allowed_dag_ids(
    api_key: Optional[str] = Depends(require_auth),
) -> Optional[Set[str]]:
    """Return the set of DAG IDs the caller may access.

    Returns:
        ``None`` when RBAC is disabled (no filtering).
        A ``set[str]`` of allowed DAG IDs when RBAC is enabled.
    """
    if not _rbac_enabled:
        return None

    if api_key is None:
        # Auth disabled but RBAC enabled — treat as full access.
        return None

    allowed = _key_dag_mapping.get(api_key)
    if allowed is None:
        # Key not in mapping — honour fail_open setting.
        if _rbac_fail_open:
            return None
        return set()
    if allowed == ["*"]:
        return None
    return set(allowed)


def check_dag_access(dag_id: str, allowed: Optional[Set[str]]) -> None:
    """Raise 403 if the caller may not access *dag_id*.

    Args:
        dag_id: The DAG being requested.
        allowed: The caller's allowed DAG set (``None`` = full access).

    Raises:
        HTTPException: 403 when access is denied.
    """
    if allowed is None:
        return
    if dag_id not in allowed:
        raise HTTPException(status_code=403, detail=error_response("Access denied for this DAG"))


def filter_dags(items: List[Dict[str, Any]], allowed: Optional[Set[str]], key: str = "dag_id") -> List[Dict[str, Any]]:
    """Filter a list of dicts to only those the caller may see.

    Args:
        items: List of dictionaries, each expected to contain *key*.
        allowed: Allowed DAG IDs (``None`` = no filtering).
        key: Dict key that holds the DAG ID.

    Returns:
        Filtered list.
    """
    if allowed is None:
        return items
    return [item for item in items if item.get(key) in allowed]
