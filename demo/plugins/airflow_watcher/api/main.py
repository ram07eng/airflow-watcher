"""Entry point for the standalone Airflow Watcher API.

Boots a FastAPI application that exposes monitoring endpoints backed by
the existing airflow-watcher monitors, connecting directly to the Airflow
metadata database via SQLAlchemy.
"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Optional, Tuple

from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response

from airflow_watcher.api.compat import install_airflow_stubs, reflect_airflow_models
from airflow_watcher.api.db import get_engine, get_read_engine, init_db
from airflow_watcher.api.envelope import error_response
from airflow_watcher.api.logging_config import (
    configure_logging,
    generate_request_id,
    set_request_id,
)
from airflow_watcher.api.standalone_config import StandaloneConfig

logger = logging.getLogger(__name__)

# Recorded at app creation time; used by /healthz.
_start_time: float = 0.0

# Background task handle for cleanup.
_statsd_task: Optional[asyncio.Task] = None


def create_app() -> Tuple[FastAPI, StandaloneConfig]:
    """Create and configure the FastAPI application.

    Returns:
        A tuple of (FastAPI app, StandaloneConfig).
    """
    global _start_time

    load_dotenv(override=False)
    config = StandaloneConfig.from_env()
    init_db(
        config.db_uri,
        query_timeout_ms=config.query_timeout_ms,
        pool_size=config.db_pool_size,
        max_overflow=config.db_max_overflow,
        db_read_uri=config.db_read_uri,
    )

    # Reflect Airflow metadata tables onto stub models (standalone mode).
    engine = get_engine()
    if engine is not None:
        try:
            reflect_airflow_models(engine)
        except Exception as exc:
            logger.warning("Could not reflect Airflow models: %s", exc)

    # --- Structured logging ---
    configure_logging(log_format=config.log_format, log_level=config.log_level)

    _start_time = time.monotonic()

    # --- Lifespan: startup / shutdown hooks (replaces deprecated on_event) ---
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        global _statsd_task
        # Startup
        if config.statsd_enabled:
            from airflow_watcher.metrics.collector import MetricsCollector
            from airflow_watcher.metrics.statsd_emitter import StatsDEmitter

            emitter = StatsDEmitter(
                host=config.statsd_host,
                port=config.statsd_port,
                prefix=config.statsd_prefix,
                use_dogstatsd=config.use_dogstatsd,
            )
            collector = MetricsCollector()

            async def _emit_loop():
                while True:
                    try:
                        metrics = collector.collect()
                        emitter.emit_watcher_metrics(metrics)
                    except Exception as exc:
                        logger.warning("StatsD emission failed: %s", exc)
                    await asyncio.sleep(config.cache_ttl)

            _statsd_task = asyncio.create_task(_emit_loop())

        yield

        # Shutdown — cancel background tasks
        if _statsd_task is not None:
            _statsd_task.cancel()
            try:
                await _statsd_task
            except asyncio.CancelledError:
                pass

    _DESCRIPTION = """\
## What is this?

**Airflow Watcher** is a monitoring API that connects directly to the Airflow metadata
database and exposes structured JSON endpoints for failure detection, SLA tracking,
task health, scheduling analysis, and dependency mapping.

---

## Key capabilities

| Endpoint group | What it answers |
|---|---|
| **Overview** | Single-call snapshot: failures, SLA, queue, health score |
| **Failures** | Which DAGs failed, failure rates, top offenders |
| **SLA** | SLA miss events, per-DAG miss rates and trends |
| **Tasks** | Long-running tasks, zombies, retry storms, failure patterns |
| **Scheduling** | Lag percentiles, queue depth, pool utilisation, stale DAGs |
| **DAGs** | Import errors, health score, complexity, inactive DAGs |
| **Dependencies** | Upstream failures, cross-DAG links, failure correlations, impact radius |
| **Health** | Composite health score (0–100); per-DAG last-10 failures + SLA misses |
| **Alerts** | Rule evaluation and notification dispatch |
| **Cache** | In-memory cache invalidation |

---

## Authentication

All `/api/v1` endpoints require an API key passed via the `Authorization: Bearer <key>`
header or the `X-API-Key` header. Configure keys via the `AIRFLOW_WATCHER_API_KEYS`
environment variable (comma-separated).

---

## RBAC

When RBAC is enabled, responses are scoped to the DAG IDs permitted for the
caller's API key. Aggregate statistics (counts, rates) are always global;
per-DAG breakdowns are filtered.

---

## Caching

Most read endpoints cache results in memory (default TTL: 60 s) to reduce
database load. Use `POST /api/v1/cache/invalidate` to flush immediately.
"""

    _TAGS = [
        {
            "name": "overview",
            "description": (
                "Single-call monitoring snapshot. Aggregates failures, SLA stats, queue depth, "
                "zombie count, DAG health score, and import errors into one response. "
                "Cached 60 s, keyed per RBAC scope."
            ),
        },
        {
            "name": "failures",
            "description": (
                "DAG run failure monitoring. `/failures/` returns raw failure rows; "
                "`/failures/stats` aggregates into rates, total model counts, and top offending DAGs."
            ),
        },
        {
            "name": "sla",
            "description": (
                "SLA miss tracking. `/sla/misses` returns individual miss events; "
                "`/sla/stats` gives miss rates and the most frequently missing DAGs and tasks."
            ),
        },
        {
            "name": "tasks",
            "description": (
                "Task-level health signals: long-running invocations, potential zombies, "
                "retry storms, and failure pattern analysis (flaky vs. consistently failing tasks)."
            ),
        },
        {
            "name": "scheduling",
            "description": (
                "Scheduling health: p50/p90/p95 lag between scheduled time and actual execution start, "
                "current queue and scheduled task counts, pool utilisation, stale DAGs, "
                "and DAGs with unexpected concurrent runs."
            ),
        },
        {
            "name": "dags",
            "description": (
                "DAG inventory and health: import/parse errors, composite health score, "
                "complexity analysis (task count, branching), and inactive DAGs that have stopped running."
            ),
        },
        {
            "name": "dependencies",
            "description": (
                "Dependency analysis: tasks stuck in `upstream_failed` state, "
                "cross-DAG sensor/trigger links (requires full Airflow install), "
                "failure co-occurrence correlations, and cascading impact radius for a given task."
            ),
        },
        {
            "name": "health",
            "description": (
                "Composite health endpoint. `GET /health/` returns HTTP 200 when `health_score ≥ 70` "
                "and no import errors, otherwise HTTP 503 with `Retry-After: 30`. "
                "`GET /health/{dag_id}` returns the last 10 failures and SLA misses for a specific DAG."
            ),
        },
        {
            "name": "alerts",
            "description": (
                "Alert rule management. `GET /alerts/rules` lists configured thresholds and channels. "
                "`POST /alerts/evaluate` runs all rules against live metrics and dispatches "
                "notifications (Slack, email, PagerDuty) for any that fire."
            ),
        },
        {
            "name": "cache",
            "description": (
                "In-memory response cache. `POST /cache/invalidate` clears all entries immediately, "
                "forcing the next request to re-query the database. Useful after a bulk backfill "
                "or when stale data is suspected."
            ),
        },
        {
            "name": "system",
            "description": (
                "Liveness probe at `/healthz` (unauthenticated). "
                "Returns DB connectivity status, read-replica connectivity, and uptime."
            ),
        },
    ]

    app = FastAPI(
        title="Airflow Watcher API",
        version="1.0",
        description=_DESCRIPTION,
        openapi_tags=_TAGS,
        lifespan=lifespan,
        contact={
            "name": "Data Engineering — JET",
            "email": "ramanujam.solaimalai@justeattakeaway.com",
        },
        license_info={
            "name": "Internal — not for public use",
        },
    )

    # Versioning: all endpoints live under /api/v1. When v2 is introduced, a
    # /api/v2 router will be added alongside v1. A ``Sunset`` header will be
    # returned on deprecated version prefixes to signal deprecation.

    # --- Configure auth & RBAC ---
    from airflow_watcher.api.auth import configure_auth
    from airflow_watcher.api.rbac_dep import configure_rbac

    configure_auth(config.api_keys)
    configure_rbac(config.rbac_enabled, config.rbac_key_dag_mapping, fail_open=config.rbac_fail_open)

    # --- Configure cache TTL ---
    from airflow_watcher.utils.cache import MetricsCache

    MetricsCache.get_instance(default_ttl=config.cache_ttl)

    # --- Rate limiting middleware (review H-6) ---
    from airflow_watcher.api.rate_limit import RateLimitMiddleware

    app.add_middleware(RateLimitMiddleware, max_requests=config.rate_limit_rpm, window_seconds=60)

    # --- Request ID middleware ---
    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next):
        rid = request.headers.get("X-Request-ID") or generate_request_id()
        set_request_id(rid)
        response: Response = await call_next(request)
        response.headers["X-Request-ID"] = rid
        if config.log_format == "json":
            logger.info(
                "%s %s %s",
                request.method,
                request.url.path,
                response.status_code,
                extra={"request_id": rid},
            )
        return response

    # --- Request timeout middleware (review H-3) ---
    @app.middleware("http")
    async def request_timeout_middleware(request: Request, call_next):
        try:
            return await asyncio.wait_for(call_next(request), timeout=config.request_timeout_seconds)
        except asyncio.TimeoutError:
            from fastapi.responses import JSONResponse

            return JSONResponse(status_code=504, content=error_response("Request timed out"))

    # --- X-API-Version middleware ---
    @app.middleware("http")
    async def add_api_version_header(request: Request, call_next):
        response: Response = await call_next(request)
        response.headers["X-API-Version"] = "1.0"
        return response

    # --- Security headers middleware ---
    _DOCS_PATHS = ("/docs", "/redoc", "/openapi.json", "/docs/oauth2-redirect")

    @app.middleware("http")
    async def add_security_headers(request: Request, call_next):
        response: Response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

        if request.url.path in _DOCS_PATHS:
            response.headers["Content-Security-Policy"] = (
                "default-src 'self'; "
                "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
                "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
                "img-src 'self' https://fastapi.tiangolo.com data:; "
                "connect-src 'self'"
            )
        else:
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["Content-Security-Policy"] = "default-src 'none'; frame-ancestors 'none'"
            response.headers["Cache-Control"] = "no-store"
        return response

    # --- Global exception handler (never leak internals) ---
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.error("Unhandled API error on %s %s", request.method, request.url.path, exc_info=True)
        from fastapi.responses import JSONResponse

        return JSONResponse(status_code=500, content=error_response("An internal error occurred"))

    # --- /healthz endpoint (minimal public info) ---
    @app.get("/healthz", tags=["system"], summary="Liveness probe", response_description="DB connectivity and uptime")
    async def healthz():
        """Unauthenticated liveness probe.

        Returns `status: ok` when the primary DB is reachable (and the read replica,
        if configured). Returns `status: degraded` otherwise.

        Safe to call from load-balancer health checks — no auth required.
        """
        uptime = time.monotonic() - _start_time
        db_connected = False
        engine = get_engine()
        if engine is not None:
            try:
                with engine.connect() as conn:
                    conn.execute(__import__("sqlalchemy").text("SELECT 1"))
                db_connected = True
            except Exception:
                db_connected = False

        read_db_connected = None
        read_engine = get_read_engine()
        if read_engine is not None and read_engine is not engine:
            try:
                with read_engine.connect() as conn:
                    conn.execute(__import__("sqlalchemy").text("SELECT 1"))
                read_db_connected = True
            except Exception:
                read_db_connected = False

        status = "ok" if db_connected and (read_db_connected is not False) else "degraded"
        result = {
            "status": status,
            "uptime_seconds": round(uptime, 2),
            "db_connected": db_connected,
        }
        if read_db_connected is not None:
            result["read_db_connected"] = read_db_connected
        return result

    # --- Register API v1 routers ---
    from fastapi import APIRouter

    from airflow_watcher.api.routers.alerts import router as alerts_router
    from airflow_watcher.api.routers.cache import router as cache_router
    from airflow_watcher.api.routers.dags import router as dags_router
    from airflow_watcher.api.routers.dependencies import router as deps_router
    from airflow_watcher.api.routers.failures import router as failures_router
    from airflow_watcher.api.routers.health import router as health_router
    from airflow_watcher.api.routers.overview import router as overview_router
    from airflow_watcher.api.routers.scheduling import router as scheduling_router
    from airflow_watcher.api.routers.sla import router as sla_router
    from airflow_watcher.api.routers.tasks import router as tasks_router

    v1 = APIRouter(prefix="/api/v1")
    v1.include_router(failures_router)
    v1.include_router(sla_router)
    v1.include_router(tasks_router)
    v1.include_router(scheduling_router)
    v1.include_router(dags_router)
    v1.include_router(deps_router)
    v1.include_router(overview_router)
    v1.include_router(health_router)
    v1.include_router(alerts_router)
    v1.include_router(cache_router)
    app.include_router(v1)

    # --- /metrics endpoint (root-level, not under /api/v1) ---
    if config.prometheus_enabled:
        from airflow_watcher.api.routers.metrics import configure_metrics
        from airflow_watcher.api.routers.metrics import router as metrics_router
        from airflow_watcher.metrics.prometheus_exporter import PrometheusExporter

        exporter = PrometheusExporter(prefix=config.prometheus_prefix)
        configure_metrics(exporter)
        app.include_router(metrics_router)

    return app, config


# Module-level ASGI app for Gunicorn/Uvicorn: gunicorn airflow_watcher.api.main:app
install_airflow_stubs()
app: Optional[FastAPI] = None
_config: Optional[StandaloneConfig] = None
try:
    app, _config = create_app()
except Exception:
    # Safe import during testing / when env vars are not set.
    pass


def main():
    """Console script entry point for ``airflow-watcher-api``."""
    import sys

    import uvicorn

    if app is None:
        logger.error("Application failed to initialise. Check AIRFLOW_WATCHER_DB_URI.")
        sys.exit(1)
    uvicorn.run(app, host=_config.api_host, port=_config.api_port)
