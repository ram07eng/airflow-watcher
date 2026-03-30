"""Tests for API v1 routers — endpoint registration, envelope format, auth gate."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from airflow_watcher.api.auth import configure_auth
from airflow_watcher.api.rbac_dep import configure_rbac


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _build_test_app(api_keys=None):
    """Build an app with all routers wired but monitors mocked."""
    configure_auth(api_keys or [])
    configure_rbac(False, {})

    # Patch monitor_provider functions so routers don't hit a real DB.
    patches = []

    def _mock_provider(func_path, methods):
        """Patch a monitor_provider getter to return a mock with preset methods."""
        mock_inst = MagicMock()
        for method_name, return_value in methods.items():
            getattr(mock_inst, method_name).return_value = return_value
        p = patch(func_path, return_value=mock_inst)
        patches.append(p)
        p.start()
        return mock_inst

    _mock_provider("airflow_watcher.api.routers.failures.get_failure_monitor", {
        "get_recent_failures": [],
        "get_failure_statistics": {"total_runs": 100, "failed_runs": 5},
        "get_dag_health_status": {"summary": {}},
    })
    _mock_provider("airflow_watcher.api.routers.sla.get_sla_monitor", {
        "get_recent_sla_misses": [],
        "get_sla_statistics": {"total_misses": 2},
    })
    _mock_provider("airflow_watcher.api.routers.tasks.get_task_monitor", {
        "get_long_running_tasks": [],
        "get_retry_heavy_tasks": [],
        "get_zombie_tasks": [],
        "get_task_failure_patterns": {"total_failures": 0},
    })
    _mock_provider("airflow_watcher.api.routers.scheduling.get_scheduling_monitor", {
        "get_scheduling_lag": {"avg": 1.5},
        "get_queued_tasks": {"queued": 0, "scheduled": 0},
        "get_pool_utilization": [],
        "get_stale_dags": [],
        "get_concurrent_runs": [],
    })
    _mock_provider("airflow_watcher.api.routers.dags.get_dag_health_monitor", {
        "get_dag_import_errors": [],
        "get_dag_status_summary": {"total_dags": 50, "health_score": 95},
        "get_dag_complexity_analysis": [],
        "get_inactive_dags": [],
    })
    _mock_provider("airflow_watcher.api.routers.dependencies.get_dependency_monitor", {
        "get_upstream_failures": [],
        "get_cross_dag_dependencies": [],
        "get_failure_correlation": {"correlations": []},
        "get_cascading_failure_impact": {"impacted": 0},
    })

    # Health router imports its own copies of the provider functions.
    _mock_provider("airflow_watcher.api.routers.health.get_dag_health_monitor", {
        "get_dag_status_summary": {"total_dags": 50, "health_score": 95},
        "get_dag_import_errors": [],
    })
    _mock_provider("airflow_watcher.api.routers.health.get_failure_monitor", {
        "get_dag_health_status": {"summary": {}},
        "get_recent_failures": [],
    })
    _mock_provider("airflow_watcher.api.routers.health.get_sla_monitor", {
        "get_recent_sla_misses": [],
    })

    # Overview router imports its own copies of the provider functions.
    _mock_provider("airflow_watcher.api.routers.overview.get_failure_monitor", {
        "get_failure_statistics": {"total_runs": 100},
    })
    _mock_provider("airflow_watcher.api.routers.overview.get_sla_monitor", {
        "get_sla_statistics": {"total_misses": 0},
    })
    _mock_provider("airflow_watcher.api.routers.overview.get_task_monitor", {
        "get_long_running_tasks": [],
        "get_zombie_tasks": [],
    })
    _mock_provider("airflow_watcher.api.routers.overview.get_scheduling_monitor", {
        "get_queued_tasks": {"queued": 0},
    })
    _mock_provider("airflow_watcher.api.routers.overview.get_dag_health_monitor", {
        "get_dag_status_summary": {"total_dags": 50},
        "get_dag_import_errors": [],
    })

    # Prevent 501 on DagBag-dependent endpoints in test mode.
    p_standalone = patch("airflow_watcher.api.routers.dependencies._is_standalone", return_value=False)
    patches.append(p_standalone)
    p_standalone.start()

    # Build a minimal app with all routers (reuse wiring logic from main.py
    # but skip DB init and config loading).
    from fastapi import APIRouter, FastAPI

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

    app = FastAPI()
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

    return app, patches


@pytest.fixture
def client():
    """App with auth disabled, monitors mocked."""
    # Clear the cache so each test gets fresh data.
    from airflow_watcher.utils.cache import MetricsCache
    MetricsCache.get_instance().clear()

    app, patches = _build_test_app()
    yield TestClient(app)
    for p in patches:
        p.stop()


@pytest.fixture
def auth_client():
    """App with auth enabled (valid key = 'test-key')."""
    from airflow_watcher.utils.cache import MetricsCache
    MetricsCache.get_instance().clear()

    app, patches = _build_test_app(api_keys=["test-key"])
    yield TestClient(app)
    for p in patches:
        p.stop()


# ---------------------------------------------------------------------------
# Envelope format
# ---------------------------------------------------------------------------

class TestEnvelopeFormat:
    """All endpoints return {status, data, timestamp}."""

    ENDPOINTS = [
        "/api/v1/failures/",
        "/api/v1/failures/stats",
        "/api/v1/sla/misses",
        "/api/v1/sla/stats",
        "/api/v1/tasks/long-running",
        "/api/v1/tasks/retries",
        "/api/v1/tasks/zombies",
        "/api/v1/tasks/failure-patterns",
        "/api/v1/scheduling/lag",
        "/api/v1/scheduling/queue",
        "/api/v1/scheduling/pools",
        "/api/v1/scheduling/stale-dags",
        "/api/v1/scheduling/concurrent",
        "/api/v1/dags/import-errors",
        "/api/v1/dags/status-summary",
        "/api/v1/dags/complexity",
        "/api/v1/dags/inactive",
        "/api/v1/dependencies/upstream-failures",
        "/api/v1/dependencies/cross-dag",
        "/api/v1/dependencies/correlations",
        "/api/v1/overview/",
        "/api/v1/health/",
    ]

    @pytest.mark.parametrize("path", ENDPOINTS)
    def test_success_envelope(self, client, path):
        resp = client.get(path)
        body = resp.json()
        assert body["status"] == "success"
        assert "data" in body
        assert "timestamp" in body


# ---------------------------------------------------------------------------
# API versioning
# ---------------------------------------------------------------------------

class TestVersioning:
    """Unversioned paths should 404."""

    def test_unversioned_failures_returns_404(self, client):
        resp = client.get("/failures")
        assert resp.status_code == 404

    def test_unversioned_sla_returns_404(self, client):
        resp = client.get("/sla/misses")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Auth gate
# ---------------------------------------------------------------------------

class TestAuthGate:
    def test_missing_auth_returns_401(self, auth_client):
        resp = auth_client.get("/api/v1/failures/")
        assert resp.status_code == 401

    def test_valid_auth_returns_200(self, auth_client):
        resp = auth_client.get(
            "/api/v1/failures/",
            headers={"Authorization": "Bearer test-key"},
        )
        assert resp.status_code == 200

    def test_invalid_auth_returns_401(self, auth_client):
        resp = auth_client.get(
            "/api/v1/failures/",
            headers={"Authorization": "Bearer wrong-key"},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Specific endpoint behaviour
# ---------------------------------------------------------------------------

class TestFailuresEndpoints:
    def test_failures_returns_count(self, client):
        resp = client.get("/api/v1/failures/")
        data = resp.json()["data"]
        assert "count" in data
        assert data["count"] == 0

    def test_failures_stats(self, client):
        resp = client.get("/api/v1/failures/stats")
        assert resp.status_code == 200


class TestHealthEndpoint:
    def test_healthy_returns_200(self, client):
        resp = client.get("/api/v1/health/")
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["status"] == "healthy"
        assert data["health_score"] == 95


class TestCacheInvalidate:
    def test_invalidate_returns_success(self, client):
        resp = client.post("/api/v1/cache/invalidate")
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["cleared"] is True


class TestOverview:
    def test_overview_returns_all_sections(self, client):
        resp = client.get("/api/v1/overview/")
        data = resp.json()["data"]
        assert "failure_stats" in data
        assert "sla_stats" in data
        assert "long_running_tasks" in data
        assert "zombie_count" in data
        assert "queue_status" in data
        assert "dag_summary" in data
        assert "import_errors" in data
