"""Simple in-memory rate limiting middleware for the standalone API."""

import asyncio
import time
from collections import defaultdict

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from airflow_watcher.api.envelope import error_response


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Per-IP sliding window rate limiter.

    Limits each client IP to *max_requests* within *window_seconds*.
    Returns HTTP 429 when the limit is exceeded.

    Note: In a multi-process deployment (gunicorn with multiple workers),
    each process maintains its own counter. Use a shared store (Redis)
    for accurate cross-process rate limiting.
    """

    def __init__(self, app, max_requests: int = 120, window_seconds: int = 60):
        super().__init__(app)
        self._max = max_requests
        self._window = window_seconds
        self._hits: dict[str, list[float]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self._last_cleanup: float = 0.0
        self._cleanup_interval: float = 300.0  # prune stale IPs every 5 min

    async def dispatch(self, request: Request, call_next):
        # Skip rate limiting for health checks
        if request.url.path == "/healthz":
            return await call_next(request)

        client_ip = request.headers.get("X-Forwarded-For", "").split(",")[0].strip() or (
            request.client.host if request.client else "unknown"
        )
        now = time.monotonic()

        async with self._lock:
            window_start = now - self._window
            # Prune old entries
            self._hits[client_ip] = [t for t in self._hits[client_ip] if t > window_start]

            if len(self._hits[client_ip]) >= self._max:
                retry_after = int(self._window - (now - self._hits[client_ip][0])) + 1
                return JSONResponse(
                    status_code=429,
                    content=error_response("Rate limit exceeded"),
                    headers={"Retry-After": str(retry_after)},
                )

            self._hits[client_ip].append(now)

            # Periodically purge IPs with no recent hits to prevent unbounded growth
            if now - self._last_cleanup > self._cleanup_interval:
                stale_ips = [ip for ip, ts in self._hits.items() if not ts or ts[-1] <= window_start]
                for ip in stale_ips:
                    del self._hits[ip]
                self._last_cleanup = now

        return await call_next(request)
