"""Role-Based Access Control utilities for Airflow Watcher.

Integrates with Airflow's FAB security manager to enforce DAG-level
access control. Teams only see DAGs they have permission to view.
"""

import logging
from typing import Any, Dict, Optional, Set

from flask import has_request_context

logger = logging.getLogger(__name__)


def get_current_user() -> Any:
    """Get the currently authenticated Airflow user.

    Returns:
        The current user object, or None if not authenticated.
    """
    if not has_request_context():
        return None

    try:
        from flask_login import current_user

        if current_user and current_user.is_authenticated:
            return current_user
    except ImportError:
        pass

    return None


def get_accessible_dag_ids(user: Any = None) -> Optional[Set[str]]:
    """Get the set of DAG IDs the current user is allowed to access.

    Uses Airflow's security manager to resolve permissions based on the
    user's assigned roles. Admin/Op users get None (no restriction).
    Regular users get a set of DAG IDs they have 'can_read' permission on
    through their roles.

    Args:
        user: Optional user object. If None, uses the current logged-in user.

    Returns:
        A set of accessible DAG IDs, or None if the user has access to all DAGs
        (admin) or if RBAC cannot be resolved (fails open for safety during migration).
    """
    if user is None:
        user = get_current_user()

    if user is None:
        return None  # No user context (e.g., CLI, tests) — no restriction

    try:
        from flask import current_app

        sm = current_app.appbuilder.sm  # type: ignore[attr-defined]

        # Admin and Op roles can see everything
        if _is_admin_user(user, sm):
            return None

        # Use Airflow's security manager to get accessible DAGs
        readable_dags = _get_readable_dag_ids(user, sm)

        if readable_dags is not None:
            logger.debug(
                "RBAC: User '%s' has access to %d DAGs",
                user.username if hasattr(user, "username") else str(user),
                len(readable_dags),
            )
            return readable_dags

    except Exception:
        logger.warning("RBAC: Could not resolve DAG permissions, failing open", exc_info=True)

    return None  # Fail open — don't break the dashboard if RBAC lookup fails


def _is_admin_user(user: Any, security_manager: Any) -> bool:
    """Check if the user has an admin-level role."""
    try:
        admin_role_names = {"Admin", "Op"}
        user_roles = {r.name for r in user.roles} if hasattr(user, "roles") else set()
        return bool(user_roles & admin_role_names)
    except Exception:
        return False


def _get_readable_dag_ids(user: Any, security_manager: Any) -> Optional[Set[str]]:
    """Get DAG IDs the user can read, using Airflow's security manager.

    Tries multiple approaches for compatibility across Airflow versions.
    """
    # Airflow 2.5+ approach
    if hasattr(security_manager, "get_accessible_dag_ids"):
        try:
            dag_ids = security_manager.get_accessible_dag_ids(user)
            return set(dag_ids) if dag_ids else set()
        except Exception:
            pass

    # Airflow 2.x approach — check DAG-level permissions
    if hasattr(security_manager, "get_user_roles"):
        try:
            return _resolve_from_permissions(user, security_manager)
        except Exception:
            pass

    return None


def _resolve_from_permissions(user: Any, security_manager: Any) -> Optional[Set[str]]:
    """Resolve accessible DAG IDs by inspecting FAB permissions directly."""
    from airflow.security import permissions as perms

    dag_ids = set()
    user_permissions = security_manager.get_user_permissions(user)

    for permission in user_permissions:
        action = permission[0] if isinstance(permission, tuple) else getattr(permission, "action", None)
        resource = permission[1] if isinstance(permission, tuple) else getattr(permission, "resource", None)

        action_name = action.name if hasattr(action, "name") else str(action)  # type: ignore[union-attr]
        resource_name = resource.name if hasattr(resource, "name") else str(resource)  # type: ignore[union-attr]

        # DAG-level read permissions look like: can_read on DAG:<dag_id>
        if action_name == perms.ACTION_CAN_READ and resource_name.startswith(perms.RESOURCE_DAG_PREFIX):
            dag_id = resource_name[len(perms.RESOURCE_DAG_PREFIX) :]
            dag_ids.add(dag_id)

    return dag_ids if dag_ids else set()


def merge_rbac_with_filters(
    rbac_dag_ids: Optional[Set[str]],
    filter_dag_ids: Optional[Set[str]],
) -> Optional[Set[str]]:
    """Merge RBAC-enforced DAG IDs with voluntary filter DAG IDs.

    RBAC is mandatory (intersection). Voluntary filters narrow further.

    Args:
        rbac_dag_ids: DAG IDs from RBAC (None = no restriction).
        filter_dag_ids: DAG IDs from tag/owner filter (None = no filter).

    Returns:
        The effective set of allowed DAG IDs, or None if unrestricted.
    """
    if rbac_dag_ids is None and filter_dag_ids is None:
        return None
    if rbac_dag_ids is None:
        return filter_dag_ids
    if filter_dag_ids is None:
        return rbac_dag_ids
    return rbac_dag_ids & filter_dag_ids


def filter_results_rbac(
    results: Any,
    allowed_dag_ids: Optional[Set[str]],
    dag_id_key: str = "dag_id",
) -> Any:
    """Filter results by allowed DAG IDs (RBAC-aware).

    Same logic as the existing filter_results, but named explicitly
    for RBAC context. Works with lists of dicts and dict-of-dicts.
    """
    if allowed_dag_ids is None:
        return results
    if isinstance(results, list):
        return [
            r
            for r in results
            if (r.get(dag_id_key) if isinstance(r, dict) else getattr(r, dag_id_key, None)) in allowed_dag_ids
        ]
    elif isinstance(results, dict):
        return {k: v for k, v in results.items() if k in allowed_dag_ids}
    return results


def get_rbac_context() -> Dict[str, Any]:
    """Get RBAC context for templates.

    Returns:
        Dict with rbac_active (bool) and rbac_user (str or None).
    """
    user = get_current_user()
    if user is None:
        return {"rbac_active": False, "rbac_user": None, "rbac_is_admin": True}

    try:
        from flask import current_app

        sm = current_app.appbuilder.sm  # type: ignore[attr-defined]
        is_admin = _is_admin_user(user, sm)
    except Exception:
        is_admin = False

    username = user.username if hasattr(user, "username") else str(user)
    return {"rbac_active": True, "rbac_user": username, "rbac_is_admin": is_admin}
