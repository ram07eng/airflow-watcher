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

from airflow_watcher.api.db import get_engine, init_db
from airflow_watcher.api.envelope import error_response
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
    init_db(config.db_uri, query_timeout_ms=config.query_timeout_ms)

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

    app = FastAPI(title="Airflow Watcher API", version="1.0", lifespan=lifespan)

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

    # --- Request timeout middleware (review H-3) ---
    @app.middleware("http")
    async def request_timeout_middleware(request: Request, call_next):
        try:
            return await asyncio.wait_for(call_next(request), timeout=60.0)
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
    @app.middleware("http")
    async def add_security_headers(request: Request, call_next):
        response: Response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Content-Security-Policy"] = "default-src 'none'; frame-ancestors 'none'"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Cache-Control"] = "no-store"
        return response

    # --- Global exception handler (never leak internals) ---
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.error("Unhandled API error on %s %s", request.method, request.url.path, exc_info=True)
        from fastapi.responses import JSONResponse

        return JSONResponse(status_code=500, content=error_response("An internal error occurred"))

    # --- /healthz endpoint (minimal public info) ---
    @app.get("/healthz")
    async def healthz():
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

        status = "ok" if db_connected else "degraded"
        return {
            "status": status,
            "uptime_seconds": round(uptime, 2),
            "db_connected": db_connected,
        }

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


def main():
    """Console script entry point for ``airflow-watcher-api``."""
    import uvicorn

    application, cfg = create_app()
    uvicorn.run(application, host=cfg.api_host, port=cfg.api_port)
