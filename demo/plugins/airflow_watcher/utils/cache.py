"""TTL-based in-memory cache for expensive monitor queries."""

import logging
import threading
import time
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

# Default TTL: 60 seconds
DEFAULT_TTL_SECONDS = 60


class MetricsCache:
    """Thread-safe TTL cache for monitor results.

    Prevents repeated expensive DB queries on every page load.
    At 4500+ DAGs, uncached queries can take 10-30 seconds each.
    """

    _instance: Optional["MetricsCache"] = None
    _lock = threading.Lock()

    def __init__(self, default_ttl: int = DEFAULT_TTL_SECONDS):
        self._cache: dict[str, dict[str, Any]] = {}
        self._default_ttl = default_ttl
        self._cache_lock = threading.Lock()

    @classmethod
    def get_instance(cls, default_ttl: int = DEFAULT_TTL_SECONDS) -> "MetricsCache":
        """Get singleton cache instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(default_ttl=default_ttl)
        return cls._instance

    def get(self, key: str) -> Optional[Any]:
        """Get cached value if not expired."""
        with self._cache_lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            if time.time() > entry["expires_at"]:
                del self._cache[key]
                return None
            return entry["value"]

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Cache a value with TTL."""
        with self._cache_lock:
            self._cache[key] = {
                "value": value,
                "expires_at": time.time() + (ttl or self._default_ttl),
            }

    def get_or_compute(self, key: str, compute_fn: Callable[[], Any], ttl: Optional[int] = None) -> Any:
        """Get from cache or compute and cache the result."""
        cached = self.get(key)
        if cached is not None:
            return cached
        value = compute_fn()
        self.set(key, value, ttl)
        return value

    def invalidate(self, key: str) -> None:
        """Remove a specific cache entry."""
        with self._cache_lock:
            self._cache.pop(key, None)

    def clear(self) -> None:
        """Clear all cached entries."""
        with self._cache_lock:
            self._cache.clear()
