"""Security and penetration tests for the Airflow Watcher API.

Covers:
- SQL injection via query/path parameters
- Auth bypass and timing attacks
- Header injection
- Path traversal
- Parameter pollution / overflow
- Information leakage
- RBAC bypass (cross-DAG data leakage)
- Cache poisoning
- Input validation bounds
"""

import time
from unittest.mock import MagicMock, patch

import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from airflow_watcher.api.auth import configure_auth
from airflow_watcher.api.rbac_dep import configure_rbac

# ---------------------------------------------------------------------------
# Shared test app builder
# ---------------------------------------------------------------------------


def _build_secure_test_app(api_keys=None, rbac_enabled=False, rbac_mapping=None):
    """Build a test app with monitors mocked and optional auth/RBAC."""
    configure_auth(api_keys or [])
    configure_rbac(rbac_enabled, rbac_mapping or {})

    patches = []

    def _mock_provider(func_path, methods):
        """Patch a monitor_provider getter to return a mock with preset methods."""
        mock_inst = MagicMock()
        for m, rv in methods.items():
            getattr(mock_inst, m).return_value = rv
        p = patch(func_path, return_value=mock_inst)
        patches.append(p)
        p.start()
        return mock_inst

    _mock_provider(
        "airflow_watcher.api.routers.failures.get_failure_monitor",
        {
            "get_recent_failures": [],
            "get_failure_statistics": {"total_runs": 10},
        },
    )
    _mock_provider(
        "airflow_watcher.api.routers.sla.get_sla_monitor",
        {
            "get_recent_sla_misses": [],
            "get_sla_statistics": {"total_misses": 0},
        },
    )
    _mock_provider(
        "airflow_watcher.api.routers.tasks.get_task_monitor",
        {
            "get_long_running_tasks": [],
            "get_retry_heavy_tasks": [],
            "get_zombie_tasks": [],
            "get_task_failure_patterns": {"total_failures": 0},
        },
    )
    _mock_provider(
        "airflow_watcher.api.routers.scheduling.get_scheduling_monitor",
        {
            "get_scheduling_lag": {},
            "get_queued_tasks": {},
            "get_pool_utilization": [],
            "get_stale_dags": [],
            "get_concurrent_runs": [],
        },
    )
    _mock_provider(
        "airflow_watcher.api.routers.dags.get_dag_health_monitor",
        {
            "get_dag_import_errors": [],
            "get_dag_status_summary": {"total_dags": 50, "health_score": 95},
            "get_dag_complexity_analysis": [],
            "get_inactive_dags": [],
        },
    )
    _mock_provider(
        "airflow_watcher.api.routers.dependencies.get_dependency_monitor",
        {
            "get_upstream_failures": [],
            "get_cross_dag_dependencies": [],
            "get_failure_correlation": {"correlations": []},
            "get_cascading_failure_impact": {
                "impacted": 2,
                "downstream_tasks": [
                    {"dag_id": "allowed_dag", "task_id": "t1"},
                    {"dag_id": "secret_dag", "task_id": "t2"},
                ],
            },
        },
    )
    _mock_provider("airflow_watcher.api.routers.overview.get_failure_monitor", {"get_failure_statistics": {}})
    _mock_provider("airflow_watcher.api.routers.overview.get_sla_monitor", {"get_sla_statistics": {}})
    _mock_provider(
        "airflow_watcher.api.routers.overview.get_task_monitor", {"get_long_running_tasks": [], "get_zombie_tasks": []}
    )
    _mock_provider("airflow_watcher.api.routers.overview.get_scheduling_monitor", {"get_queued_tasks": {}})
    _mock_provider(
        "airflow_watcher.api.routers.overview.get_dag_health_monitor",
        {"get_dag_status_summary": {}, "get_dag_import_errors": []},
    )
    _mock_provider(
        "airflow_watcher.api.routers.health.get_dag_health_monitor",
        {"get_dag_status_summary": {"health_score": 95}, "get_dag_import_errors": []},
    )
    _mock_provider(
        "airflow_watcher.api.routers.health.get_failure_monitor",
        {"get_dag_health_status": {"summary": {}}, "get_recent_failures": []},
    )
    _mock_provider("airflow_watcher.api.routers.health.get_sla_monitor", {"get_recent_sla_misses": []})

    # Prevent 501 on DagBag-dependent endpoints in test mode.
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
    for r in (
        failures_router,
        sla_router,
        tasks_router,
        scheduling_router,
        dags_router,
        deps_router,
        overview_router,
        health_router,
        alerts_router,
        cache_router,
    ):
        v1.include_router(r)
    app.include_router(v1)

    return app, patches


@pytest.fixture
def secure_client():
    """App with auth enabled (key='sec-key')."""
    from airflow_watcher.utils.cache import MetricsCache

    MetricsCache.get_instance().clear()

    app, patches = _build_secure_test_app(api_keys=["sec-key"])
    yield TestClient(app)
    for p in patches:
        p.stop()


@pytest.fixture
def open_client():
    """App with auth disabled."""
    from airflow_watcher.utils.cache import MetricsCache

    MetricsCache.get_instance().clear()

    app, patches = _build_secure_test_app()
    yield TestClient(app)
    for p in patches:
        p.stop()


@pytest.fixture
def rbac_client():
    """App with RBAC enabled — key maps to only 'allowed_dag'."""
    from airflow_watcher.utils.cache import MetricsCache

    MetricsCache.get_instance().clear()

    app, patches = _build_secure_test_app(
        api_keys=["rbac-key"],
        rbac_enabled=True,
        rbac_mapping={"rbac-key": ["allowed_dag"]},
    )
    yield TestClient(app)
    for p in patches:
        p.stop()


AUTH = {"Authorization": "Bearer sec-key"}
RBAC_AUTH = {"Authorization": "Bearer rbac-key"}


# =====================================================================
# 1. SQL INJECTION TESTS
# =====================================================================


class TestSQLInjection:
    """Verify that SQL injection payloads in query/path params are handled safely."""

    PAYLOADS = [
        "'; DROP TABLE dag_run; --",
        "1 OR 1=1",
        "' UNION SELECT * FROM users --",
        "1; EXEC xp_cmdshell('whoami')",
        "' OR '1'='1",
        "admin'--",
        "1' AND SLEEP(5)--",
        "{{7*7}}",  # SSTI
        "${7*7}",  # Template injection
    ]

    @pytest.mark.parametrize("payload", PAYLOADS)
    def test_dag_id_injection_failures(self, open_client, payload):
        """dag_id param should not cause 500/crash with SQL payloads."""
        resp = open_client.get("/api/v1/failures/", params={"dag_id": payload})
        assert resp.status_code in (200, 400, 422), f"Unexpected {resp.status_code} for payload: {payload}"

    @pytest.mark.parametrize("payload", PAYLOADS)
    def test_dag_id_injection_sla(self, open_client, payload):
        resp = open_client.get("/api/v1/sla/misses", params={"dag_id": payload})
        assert resp.status_code in (200, 400, 422)

    @pytest.mark.parametrize("payload", PAYLOADS)
    def test_path_param_injection_health(self, open_client, payload):
        """dag_id in path params should not cause crashes."""
        resp = open_client.get(f"/api/v1/health/{payload}")
        assert resp.status_code in (200, 400, 403, 404, 422)

    @pytest.mark.parametrize("payload", PAYLOADS)
    def test_path_param_injection_impact(self, open_client, payload):
        resp = open_client.get(f"/api/v1/dependencies/impact/{payload}/{payload}")
        assert resp.status_code in (200, 400, 403, 404, 422)


# =====================================================================
# 2. AUTHENTICATION BYPASS TESTS
# =====================================================================


class TestAuthBypass:
    """Attempt to bypass authentication."""

    def test_no_header(self, secure_client):
        resp = secure_client.get("/api/v1/failures/")
        assert resp.status_code == 401

    def test_empty_bearer(self, secure_client):
        resp = secure_client.get("/api/v1/failures/", headers={"Authorization": "Bearer "})
        assert resp.status_code == 401

    def test_wrong_scheme_basic(self, secure_client):
        resp = secure_client.get("/api/v1/failures/", headers={"Authorization": "Basic c2VjLWtleTo="})
        assert resp.status_code == 401

    def test_wrong_scheme_token(self, secure_client):
        resp = secure_client.get("/api/v1/failures/", headers={"Authorization": "Token sec-key"})
        assert resp.status_code == 401

    def test_null_byte_in_token(self, secure_client):
        resp = secure_client.get("/api/v1/failures/", headers={"Authorization": "Bearer sec-key\x00extra"})
        assert resp.status_code == 401

    def test_unicode_homoglyph_key(self, secure_client):
        """Homoglyph attack: replace 'e' with Cyrillic 'е' (U+0435).

        httpx rejects non-ASCII header values at the transport layer,
        which is itself a security win — the attack never reaches our auth code.
        """
        import httpx

        with pytest.raises((UnicodeEncodeError, httpx.InvalidURL)):
            secure_client.get("/api/v1/failures/", headers={"Authorization": "Bearer s\u0435c-key"})

    def test_case_variation(self, secure_client):
        resp = secure_client.get("/api/v1/failures/", headers={"Authorization": "Bearer SEC-KEY"})
        assert resp.status_code == 401

    def test_trailing_whitespace(self, secure_client):
        # Trailing whitespace is stripped — token should be accepted.
        resp = secure_client.get("/api/v1/failures/", headers={"Authorization": "Bearer sec-key "})
        assert resp.status_code == 200

    def test_leading_whitespace(self, secure_client):
        # Leading whitespace is stripped — token should be accepted.
        resp = secure_client.get("/api/v1/failures/", headers={"Authorization": "Bearer  sec-key"})
        assert resp.status_code == 200

    def test_double_bearer(self, secure_client):
        resp = secure_client.get("/api/v1/failures/", headers={"Authorization": "Bearer Bearer sec-key"})
        assert resp.status_code == 401

    def test_valid_key_succeeds(self, secure_client):
        resp = secure_client.get("/api/v1/failures/", headers=AUTH)
        assert resp.status_code == 200


# =====================================================================
# 3. TIMING ATTACK RESISTANCE
# =====================================================================


class TestTimingAttack:
    """Verify constant-time comparison doesn't leak valid key prefixes."""

    def test_timing_consistency(self, secure_client):
        """Response time for wrong keys shouldn't vary with prefix-match length.

        This is a probabilistic test — we measure 50 requests for a fully
        wrong key vs a partially correct key and check they're in the same
        ballpark (within 3x).  Not a guarantee but a good regression signal.
        """

        def _measure(token, n=50):
            times = []
            for _ in range(n):
                start = time.perf_counter()
                secure_client.get("/api/v1/failures/", headers={"Authorization": f"Bearer {token}"})
                times.append(time.perf_counter() - start)
            return sum(times) / len(times)

        avg_wrong = _measure("XXXXXXX")
        avg_prefix = _measure("sec-keX")  # 6/7 chars match
        avg_short = _measure("s")

        # Within 3x is a reasonable tolerance for non-constant-time noise.
        assert avg_prefix < avg_wrong * 3, "Partially-matching key took significantly longer"
        assert avg_short < avg_wrong * 3, "Short key took significantly longer"


# =====================================================================
# 4. HEADER INJECTION
# =====================================================================


class TestHeaderInjection:
    """Verify response headers can't be injected with CRLF."""

    def test_crlf_in_query(self, open_client):
        resp = open_client.get("/api/v1/failures/", params={"dag_id": "test\r\nX-Injected: true"})
        assert "X-Injected" not in resp.headers

    def test_crlf_in_auth_header(self, secure_client):
        resp = secure_client.get(
            "/api/v1/failures/",
            headers={"Authorization": "Bearer sec-key\r\nX-Injected: true"},
        )
        # Should either reject or not reflect the injected header
        assert "X-Injected" not in resp.headers


# =====================================================================
# 5. INPUT VALIDATION / DoS VECTORS
# =====================================================================


class TestInputValidation:
    """Verify bounds enforcement on query parameters."""

    def test_hours_too_large_rejected(self, open_client):
        resp = open_client.get("/api/v1/failures/", params={"hours": 999999})
        assert resp.status_code == 422

    def test_hours_zero_rejected(self, open_client):
        resp = open_client.get("/api/v1/failures/", params={"hours": 0})
        assert resp.status_code == 422

    def test_hours_negative_rejected(self, open_client):
        resp = open_client.get("/api/v1/failures/", params={"hours": -1})
        assert resp.status_code == 422

    def test_limit_too_large_rejected(self, open_client):
        resp = open_client.get("/api/v1/failures/", params={"limit": 1000})
        assert resp.status_code == 422

    def test_threshold_too_large_rejected(self, open_client):
        resp = open_client.get("/api/v1/tasks/long-running", params={"threshold_minutes": 99999})
        assert resp.status_code == 422

    def test_days_too_large_rejected(self, open_client):
        resp = open_client.get("/api/v1/dags/inactive", params={"days": 9999})
        assert resp.status_code == 422

    def test_min_retries_too_large_rejected(self, open_client):
        resp = open_client.get("/api/v1/tasks/retries", params={"min_retries": 999})
        assert resp.status_code == 422

    def test_non_integer_hours(self, open_client):
        resp = open_client.get("/api/v1/failures/", params={"hours": "abc"})
        assert resp.status_code == 422

    def test_extremely_long_dag_id(self, open_client):
        """A 1MB dag_id string should be rejected, not cached.

        httpx itself may reject the URL as too long before it reaches
        our validation layer — either outcome is acceptable.
        """
        import httpx

        try:
            resp = open_client.get("/api/v1/failures/", params={"dag_id": "A" * 1_000_000})
            assert resp.status_code == 422
        except httpx.InvalidURL:
            pass  # transport-level rejection is also a valid defence


# =====================================================================
# 6. INFORMATION LEAKAGE
# =====================================================================


class TestInformationLeakage:
    """Ensure no internal details leak in error responses."""

    def test_404_no_stack_trace(self, open_client):
        resp = open_client.get("/nonexistent/path")
        body = resp.text
        assert "Traceback" not in body
        assert "File " not in body

    def test_422_no_internal_path(self, open_client):
        resp = open_client.get("/api/v1/failures/", params={"hours": "notanint"})
        body = resp.text
        assert "/Users/" not in body
        assert "site-packages" not in body

    def test_401_error_body_generic(self, secure_client):
        resp = secure_client.get("/api/v1/failures/")
        body = resp.json()["detail"]
        assert body["message"] == "Authentication required"
        assert "key" not in body["message"].lower()
        assert "token" not in body["message"].lower()


# =====================================================================
# 7. RBAC BYPASS / CROSS-DAG LEAKAGE
# =====================================================================


class TestRBACBypass:
    """Verify RBAC can't be circumvented to leak forbidden DAG data."""

    def test_forbidden_dag_in_failures(self, rbac_client):
        """Requesting a forbidden dag_id should 403."""
        resp = rbac_client.get(
            "/api/v1/failures/",
            params={"dag_id": "secret_dag"},
            headers=RBAC_AUTH,
        )
        assert resp.status_code == 403

    def test_forbidden_dag_health(self, rbac_client):
        resp = rbac_client.get("/api/v1/health/secret_dag", headers=RBAC_AUTH)
        assert resp.status_code == 403

    def test_impact_filters_downstream_dags(self, rbac_client):
        """Cross-DAG impact should filter out dags the caller can't see."""
        resp = rbac_client.get(
            "/api/v1/dependencies/impact/allowed_dag/some_task",
            headers=RBAC_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        downstream = data.get("downstream_tasks", [])
        for item in downstream:
            if isinstance(item, dict) and "dag_id" in item:
                assert item["dag_id"] == "allowed_dag", f"Leaked forbidden dag: {item['dag_id']}"


# =====================================================================
# 8. CACHE POISONING
# =====================================================================


class TestCachePoisoning:
    """Ensure attacker-controlled params don't exhaust cache memory."""

    def test_varied_params_produce_bounded_keys(self, open_client):
        """Sending many unique param combos shouldn't crash."""
        for i in range(100):
            resp = open_client.get("/api/v1/failures/", params={"hours": i + 1})
            assert resp.status_code == 200

    def test_cache_invalidation_works(self, open_client):
        resp = open_client.post("/api/v1/cache/invalidate")
        assert resp.status_code == 200
        assert resp.json()["data"]["cleared"] is True


# =====================================================================
# 9. PATH TRAVERSAL
# =====================================================================


class TestPathTraversal:
    """Ensure path params can't escape the intended scope."""

    TRAVERSAL_PAYLOADS = [
        "../../../etc/passwd",
        "..%2F..%2Fetc%2Fpasswd",
        "....//....//etc/passwd",
        "%2e%2e%2f%2e%2e%2fetc%2fpasswd",
        "dag_id/../../admin",
    ]

    @pytest.mark.parametrize("payload", TRAVERSAL_PAYLOADS)
    def test_path_traversal_health(self, open_client, payload):
        resp = open_client.get(f"/api/v1/health/{payload}")
        # Should not return 500 or expose files
        assert resp.status_code in (200, 400, 404, 422)
        assert "root:" not in resp.text  # no /etc/passwd content


# =====================================================================
# 10. PARAMETER POLLUTION
# =====================================================================


class TestParameterPollution:
    """Test behavior with duplicate or conflicting query params."""

    def test_duplicate_hours(self, open_client):
        resp = open_client.get("/api/v1/failures/?hours=24&hours=48")
        # FastAPI takes the last value — should still succeed or 422
        assert resp.status_code in (200, 422)

    def test_extra_unknown_params_ignored(self, open_client):
        resp = open_client.get("/api/v1/failures/", params={"hours": 24, "evil_param": "DROP TABLE"})
        assert resp.status_code == 200
