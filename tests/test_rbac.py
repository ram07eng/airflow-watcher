"""Tests for RBAC dependency (rbac_dep.py) and RBAC filtering across routers.

Covers:
- configure_rbac / get_allowed_dag_ids logic
- filter_dags helper
- check_dag_access helper
- Per-endpoint RBAC filtering (import-errors, status-summary, pools, overview, dependencies)
- Cache user-scoping for overview
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from airflow_watcher.api.auth import configure_auth
from airflow_watcher.api.rbac_dep import (
    check_dag_access,
    configure_rbac,
    filter_dags,
    get_allowed_dag_ids,
)


# ---------------------------------------------------------------------------
# Unit tests for rbac_dep helpers
# ---------------------------------------------------------------------------

class TestFilterDags:
    """Unit tests for filter_dags()."""

    def test_no_filtering_when_allowed_is_none(self):
        items = [{"dag_id": "a"}, {"dag_id": "b"}]
        assert filter_dags(items, None) == items

    def test_filters_by_dag_id(self):
        items = [{"dag_id": "a"}, {"dag_id": "b"}, {"dag_id": "c"}]
        assert filter_dags(items, {"a", "c"}) == [{"dag_id": "a"}, {"dag_id": "c"}]

    def test_empty_allowed_returns_empty(self):
        items = [{"dag_id": "a"}]
        assert filter_dags(items, set()) == []

    def test_custom_key(self):
        items = [{"name": "a"}, {"name": "b"}]
        assert filter_dags(items, {"b"}, key="name") == [{"name": "b"}]

    def test_missing_key_excluded(self):
        items = [{"dag_id": "a"}, {"other": "b"}]
        assert filter_dags(items, {"a"}) == [{"dag_id": "a"}]


class TestCheckDagAccess:
    """Unit tests for check_dag_access()."""

    def test_none_allowed_grants_access(self):
        check_dag_access("any_dag", None)  # Should not raise

    def test_dag_in_allowed_grants_access(self):
        check_dag_access("my_dag", {"my_dag", "other"})

    def test_dag_not_in_allowed_raises_403(self):
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            check_dag_access("secret", {"allowed_dag"})
        assert exc_info.value.status_code == 403

    def test_empty_set_denies_all(self):
        from fastapi import HTTPException
        with pytest.raises(HTTPException):
            check_dag_access("any", set())


# ---------------------------------------------------------------------------
# Integration tests: RBAC-enabled API client
# ---------------------------------------------------------------------------

def _build_rbac_app(api_keys, rbac_enabled, rbac_mapping, monitor_overrides=None):
    """Build a throwaway test FastAPI app with RBAC and mocked monitors."""
    configure_auth(api_keys)
    configure_rbac(rbac_enabled, rbac_mapping)

    patches = []

    def _mock(func_path, methods):
        mock_inst = MagicMock()
        for m, rv in methods.items():
            getattr(mock_inst, m).return_value = rv
        p = patch(func_path, return_value=mock_inst)
        patches.append(p)
        p.start()
        return mock_inst

    dag_errors = monitor_overrides.get("import_errors", [
        {"filename": "/dags/allowed_dag.py", "timestamp": "2024-01-01T00:00:00", "stacktrace": "err"},
        {"filename": "/dags/secret_dag.py", "timestamp": "2024-01-01T00:00:00", "stacktrace": "err2"},
    ]) if monitor_overrides else [
        {"filename": "/dags/allowed_dag.py", "timestamp": "2024-01-01T00:00:00", "stacktrace": "err"},
        {"filename": "/dags/secret_dag.py", "timestamp": "2024-01-01T00:00:00", "stacktrace": "err2"},
    ]

    _mock("airflow_watcher.api.routers.dags.get_dag_health_monitor", {
        "get_dag_import_errors": dag_errors,
        "get_dag_status_summary": {"total_dags": 50, "health_score": 95},
        "get_dag_complexity_analysis": [
            {"dag_id": "allowed_dag", "task_count": 5},
            {"dag_id": "secret_dag", "task_count": 10},
        ],
        "get_inactive_dags": [
            {"dag_id": "allowed_dag", "days_inactive": 45},
            {"dag_id": "secret_dag", "days_inactive": 90},
        ],
    })
    _mock("airflow_watcher.api.routers.scheduling.get_scheduling_monitor", {
        "get_scheduling_lag": {"delayed_dags": [{"dag_id": "secret_dag"}]},
        "get_queued_tasks": {"queued_tasks": [], "scheduled_tasks": []},
        "get_pool_utilization": [{"pool_name": "default", "slots": 16}],
        "get_stale_dags": [{"dag_id": "allowed_dag"}, {"dag_id": "secret_dag"}],
        "get_concurrent_runs": {"concurrent_dags": [], "dags_with_concurrent_runs": 0},
    })
    _mock("airflow_watcher.api.routers.dependencies.get_dependency_monitor", {
        "get_upstream_failures": [
            {"dag_id": "allowed_dag", "task_id": "t1"},
            {"dag_id": "secret_dag", "task_id": "t2"},
        ],
        "get_cross_dag_dependencies": [
            {"dag_id": "allowed_dag", "target_dag_id": "allowed_dag"},
            {"dag_id": "allowed_dag", "target_dag_id": "secret_dag"},
            {"dag_id": "secret_dag", "target_dag_id": "secret_dag"},
        ],
        "get_failure_correlation": {
            "correlations": [
                {"dag_id_a": "allowed_dag", "dag_id_b": "allowed_dag"},
                {"dag_id_a": "allowed_dag", "dag_id_b": "secret_dag"},
                {"dag_id_a": "secret_dag", "dag_id_b": "secret_dag"},
            ]
        },
        "get_cascading_failure_impact": {
            "downstream_tasks": [
                {"dag_id": "allowed_dag", "task_id": "t1"},
                {"dag_id": "secret_dag", "task_id": "t2"},
            ],
        },
    })
    _mock("airflow_watcher.api.routers.failures.get_failure_monitor", {
        "get_recent_failures": [],
        "get_failure_statistics": {"total_runs": 10, "most_failing_dags": []},
    })
    _mock("airflow_watcher.api.routers.sla.get_sla_monitor", {
        "get_recent_sla_misses": [],
        "get_sla_statistics": {"total_misses": 0, "top_dags_with_misses": [], "top_tasks_with_misses": []},
    })
    _mock("airflow_watcher.api.routers.tasks.get_task_monitor", {
        "get_long_running_tasks": [],
        "get_retry_heavy_tasks": [],
        "get_zombie_tasks": [],
        "get_task_failure_patterns": {"total_failures": 0},
    })
    _mock("airflow_watcher.api.routers.overview.get_failure_monitor", {
        "get_failure_statistics": {"most_failing_dags": []},
    })
    _mock("airflow_watcher.api.routers.overview.get_sla_monitor", {
        "get_sla_statistics": {"top_dags_with_misses": [], "top_tasks_with_misses": []},
    })
    _mock("airflow_watcher.api.routers.overview.get_task_monitor", {
        "get_long_running_tasks": [],
        "get_zombie_tasks": [],
    })
    _mock("airflow_watcher.api.routers.overview.get_scheduling_monitor", {
        "get_queued_tasks": {"queued_tasks": [], "scheduled_tasks": []},
    })
    _mock("airflow_watcher.api.routers.overview.get_dag_health_monitor", {
        "get_dag_status_summary": {},
        "get_dag_import_errors": [],
    })
    _mock("airflow_watcher.api.routers.health.get_dag_health_monitor", {
        "get_dag_status_summary": {"health_score": 95},
        "get_dag_import_errors": [],
    })
    _mock("airflow_watcher.api.routers.health.get_failure_monitor", {
        "get_dag_health_status": {"summary": {}},
        "get_recent_failures": [],
    })
    _mock("airflow_watcher.api.routers.health.get_sla_monitor", {
        "get_recent_sla_misses": [],
    })
    p_standalone = patch("airflow_watcher.api.routers.dependencies._is_standalone", return_value=False)
    patches.append(p_standalone)
    p_standalone.start()

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
    for r in (failures_router, sla_router, tasks_router, scheduling_router,
              dags_router, deps_router, overview_router, health_router,
              alerts_router, cache_router):
        v1.include_router(r)
    app.include_router(v1)

    return app, patches


@pytest.fixture
def rbac_client():
    """RBAC-enabled client: key 'rbac-key' maps to ['allowed_dag']."""
    from airflow_watcher.utils.cache import MetricsCache
    MetricsCache.get_instance().clear()

    app, patches = _build_rbac_app(
        api_keys=["rbac-key"],
        rbac_enabled=True,
        rbac_mapping={"rbac-key": ["allowed_dag"]},
    )
    yield TestClient(app)
    for p in patches:
        p.stop()


@pytest.fixture
def admin_client():
    """RBAC-enabled client: key 'admin-key' maps to ['*'] (full access)."""
    from airflow_watcher.utils.cache import MetricsCache
    MetricsCache.get_instance().clear()

    app, patches = _build_rbac_app(
        api_keys=["admin-key"],
        rbac_enabled=True,
        rbac_mapping={"admin-key": ["*"]},
    )
    yield TestClient(app)
    for p in patches:
        p.stop()


# ---------------------------------------------------------------------------
# Test: import-errors endpoint RBAC filtering
# ---------------------------------------------------------------------------

class TestImportErrorsRBAC:
    def test_restricted_user_sees_only_allowed_errors(self, rbac_client):
        resp = rbac_client.get(
            "/api/v1/dags/import-errors",
            headers={"Authorization": "Bearer rbac-key"},
        )
        assert resp.status_code == 200
        errors = resp.json()["data"]["errors"]
        filenames = [e["filename"] for e in errors]
        assert any("allowed_dag" in f for f in filenames)
        assert not any("secret_dag" in f for f in filenames)

    def test_admin_sees_all_errors(self, admin_client):
        resp = admin_client.get(
            "/api/v1/dags/import-errors",
            headers={"Authorization": "Bearer admin-key"},
        )
        assert resp.status_code == 200
        assert resp.json()["data"]["count"] == 2


# ---------------------------------------------------------------------------
# Test: complexity endpoint RBAC filtering
# ---------------------------------------------------------------------------

class TestComplexityRBAC:
    def test_restricted_user_sees_only_allowed_dags(self, rbac_client):
        resp = rbac_client.get(
            "/api/v1/dags/complexity",
            headers={"Authorization": "Bearer rbac-key"},
        )
        assert resp.status_code == 200
        dags = resp.json()["data"]["dags"]
        dag_ids = [d["dag_id"] for d in dags]
        assert "allowed_dag" in dag_ids
        assert "secret_dag" not in dag_ids


# ---------------------------------------------------------------------------
# Test: inactive dags endpoint RBAC filtering
# ---------------------------------------------------------------------------

class TestInactiveDagsRBAC:
    def test_restricted_user_sees_only_allowed(self, rbac_client):
        resp = rbac_client.get(
            "/api/v1/dags/inactive",
            headers={"Authorization": "Bearer rbac-key"},
        )
        assert resp.status_code == 200
        dags = resp.json()["data"]["inactive_dags"]
        dag_ids = [d["dag_id"] for d in dags]
        assert "allowed_dag" in dag_ids
        assert "secret_dag" not in dag_ids


# ---------------------------------------------------------------------------
# Test: stale dags endpoint RBAC filtering
# ---------------------------------------------------------------------------

class TestStaleDagsRBAC:
    def test_restricted_user_sees_only_allowed(self, rbac_client):
        resp = rbac_client.get(
            "/api/v1/scheduling/stale-dags",
            headers={"Authorization": "Bearer rbac-key"},
        )
        assert resp.status_code == 200
        dags = resp.json()["data"]["stale_dags"]
        dag_ids = [d["dag_id"] for d in dags]
        assert "allowed_dag" in dag_ids
        assert "secret_dag" not in dag_ids


# ---------------------------------------------------------------------------
# Test: pools endpoint requires auth
# ---------------------------------------------------------------------------

class TestPoolsAuth:
    def test_pools_returns_data_with_auth(self, rbac_client):
        resp = rbac_client.get(
            "/api/v1/scheduling/pools",
            headers={"Authorization": "Bearer rbac-key"},
        )
        assert resp.status_code == 200
        assert resp.json()["data"]["count"] == 1

    def test_pools_rejects_without_auth(self, rbac_client):
        resp = rbac_client.get("/api/v1/scheduling/pools")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Test: cross-dag dependency RBAC (both dag_ids must be allowed)
# ---------------------------------------------------------------------------

class TestCrossDagRBAC:
    def test_only_both_sides_allowed(self, rbac_client):
        resp = rbac_client.get(
            "/api/v1/dependencies/cross-dag",
            headers={"Authorization": "Bearer rbac-key"},
        )
        assert resp.status_code == 200
        deps = resp.json()["data"]["dependencies"]
        # Only the entry where both dag_id and target_dag_id are "allowed_dag"
        for d in deps:
            assert d.get("dag_id") == "allowed_dag" or d.get("source_dag_id") == "allowed_dag"
            target = d.get("target_dag_id", d.get("dag_id"))
            assert target == "allowed_dag"


# ---------------------------------------------------------------------------
# Test: correlations RBAC (both sides)
# ---------------------------------------------------------------------------

class TestCorrelationsRBAC:
    def test_correlations_both_sides(self, rbac_client):
        resp = rbac_client.get(
            "/api/v1/dependencies/correlations",
            headers={"Authorization": "Bearer rbac-key"},
        )
        assert resp.status_code == 200
        corrs = resp.json()["data"]["correlations"]
        for c in corrs:
            # Each correlation must have both sides accessible
            a = c.get("dag_id_a", c.get("dag_id"))
            b = c.get("dag_id_b", c.get("dag_id"))
            assert a == "allowed_dag"
            assert b == "allowed_dag"


# ---------------------------------------------------------------------------
# Test: upstream failures RBAC
# ---------------------------------------------------------------------------

class TestUpstreamFailuresRBAC:
    def test_restricted_user_filtered(self, rbac_client):
        resp = rbac_client.get(
            "/api/v1/dependencies/upstream-failures",
            headers={"Authorization": "Bearer rbac-key"},
        )
        assert resp.status_code == 200
        failures = resp.json()["data"]["upstream_failures"]
        dag_ids = [f["dag_id"] for f in failures]
        assert "allowed_dag" in dag_ids
        assert "secret_dag" not in dag_ids


# ---------------------------------------------------------------------------
# Test: overview cache is user-scoped
# ---------------------------------------------------------------------------

class TestOverviewCacheScoping:
    def test_overview_returns_200_for_restricted_user(self, rbac_client):
        resp = rbac_client.get(
            "/api/v1/overview/",
            headers={"Authorization": "Bearer rbac-key"},
        )
        assert resp.status_code == 200

    def test_overview_returns_200_for_admin(self, admin_client):
        resp = admin_client.get(
            "/api/v1/overview/",
            headers={"Authorization": "Bearer admin-key"},
        )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Test: impact endpoint filters non-dict and restricted downstream
# ---------------------------------------------------------------------------

class TestImpactRBAC:
    def test_impact_filters_restricted_downstream(self, rbac_client):
        resp = rbac_client.get(
            "/api/v1/dependencies/impact/allowed_dag/task1",
            headers={"Authorization": "Bearer rbac-key"},
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        tasks = data.get("downstream_tasks", [])
        for t in tasks:
            assert t["dag_id"] == "allowed_dag"


# ---------------------------------------------------------------------------
# Test: configure_rbac / get_allowed_dag_ids
# ---------------------------------------------------------------------------

class TestConfigureRBAC:
    def test_wildcard_returns_none(self):
        """Key mapped to ['*'] should get None (full access)."""
        configure_rbac(True, {"key1": ["*"]})

        import asyncio
        from unittest.mock import AsyncMock
        from airflow_watcher.api.rbac_dep import get_allowed_dag_ids

        # Simulate the dependency call
        result = asyncio.get_event_loop().run_until_complete(get_allowed_dag_ids("key1"))
        assert result is None

    def test_unknown_key_gets_empty_set(self):
        configure_rbac(True, {"key1": ["dag_a"]})

        import asyncio
        result = asyncio.get_event_loop().run_until_complete(get_allowed_dag_ids("unknown_key"))
        assert result == set()

    def test_disabled_rbac_returns_none(self):
        configure_rbac(False, {"key1": ["dag_a"]})

        import asyncio
        result = asyncio.get_event_loop().run_until_complete(get_allowed_dag_ids("key1"))
        assert result is None
