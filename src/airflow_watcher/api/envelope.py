"""Response envelope helpers for the Airflow Watcher API.

Provides consistent JSON response structures for all API endpoints.
"""

from datetime import datetime, timezone
from typing import Any


def success_response(data: Any) -> dict:
    """Wrap data in a standard success envelope.

    Args:
        data: The response payload.

    Returns:
        ``{"status": "success", "data": ..., "timestamp": "..."}``
    """
    return {
        "status": "success",
        "data": data,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def error_response(message: str) -> dict:
    """Wrap an error message in a standard error envelope.

    Args:
        message: Human-readable error description.

    Returns:
        ``{"status": "error", "error": ..., "timestamp": "..."}``
    """
    return {
        "status": "error",
        "message": message,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
