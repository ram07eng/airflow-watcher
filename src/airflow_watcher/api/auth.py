"""Bearer token authentication dependency for the Airflow Watcher API."""

import logging
import secrets
from typing import Optional

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from airflow_watcher.api.envelope import error_response

logger = logging.getLogger(__name__)

# Module-level reference set during app startup.
_api_keys: list[str] = []

# Security scheme — shows the Authorize (lock) button in Swagger UI.
_bearer_scheme = HTTPBearer(auto_error=False)


def configure_auth(api_keys: list[str]) -> None:
    """Set the accepted API keys.  Called once at startup."""
    global _api_keys
    _api_keys = list(api_keys)
    if not _api_keys:
        logger.warning("API_KEYS not configured — authentication is DISABLED. Set AIRFLOW_WATCHER_API_KEYS to enable.")


async def require_auth(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer_scheme),
) -> Optional[str]:
    """FastAPI dependency that validates the ``Authorization: Bearer <token>`` header.

    Uses constant-time comparison to prevent timing side-channel attacks.
    When ``_api_keys`` is empty authentication is disabled and all requests
    pass through.

    Returns:
        The validated API key string, or ``None`` when auth is disabled.

    Raises:
        HTTPException: 401 when the token is missing or invalid.
    """
    if not _api_keys:
        return None  # Auth disabled

    if credentials is None:
        raise HTTPException(status_code=401, detail=error_response("Authentication required"))

    token = credentials.credentials

    # Constant-time comparison prevents timing attacks.
    matched_key = None
    for key in _api_keys:
        if secrets.compare_digest(token, key):
            matched_key = key
            break

    if matched_key is None:
        raise HTTPException(status_code=401, detail=error_response("Authentication required"))

    return matched_key
