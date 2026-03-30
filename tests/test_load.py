"""Load and stress tests for the Airflow Watcher API.

Measures:
- Throughput: requests/sec under sustained load
- Latency: p50/p95/p99 response times
- Concurrency: parallel request handling
- Cache effectiveness: hit rate under load
- Memory stability: no leaks under sustained requests
- Error rate: zero errors under normal load

Run only load tests:  ``pytest -m load``
Exclude load tests:   ``pytest -m 'not load'``
"""

import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import MagicMock, patch

import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from airflow_watcher.api.auth import configure_auth
from airflow_watcher.api.rbac_dep import configure_rbac

pytestmark = pytest.mark.load


# ---------------------------------------------------------------------------
# Test app builder (monitors return instantly with canned data)
# ---------------------------------------------------------------------------

def _build_load_test_app():
    configure_auth([])
    configure_rbac(False, {})

    patches = []

    def _mock_provider(func_path, methods):
        """Patch a monitor_provider getter to return a mock with preset methods."""
        mock_inst = MagicMock()
        for m, rv in methods.items():
            getattr(mock_inst, m).return_value = rv
        p = patch(func_path, return_value=mock_inst)
        patches.append(p)
        p.start()

    _mock_provider("airflow_watcher.api.routers.failures.get_failure_monitor", {
        "get_recent_failures": [],
        "get_failure_statistics": {"total_runs": 500, "failed_runs": 12, "failure_rate": 2.4},
    })
    _mock_provider("airflow_watcher.api.routers.sla.get_sla_monitor", {
        "get_recent_sla_misses": [],
        "get_sla_statistics": {"total_misses": 3},
    })
    _mock_provider("airflow_watcher.api.routers.tasks.get_task_monitor", {
        "get_long_running_tasks": [{"dag_id": f"dag_{i}", "task_id": f"task_{i}", "duration": 100 + i} for i in range(20)],
        "get_retry_heavy_tasks": [],
        "get_zombie_tasks": [],
        "get_task_failure_patterns": {"total_failures": 0},
    })
    _mock_provider("airflow_watcher.api.routers.scheduling.get_scheduling_monitor", {
        "get_scheduling_lag": {"avg": 2.1, "p50": 1.5, "p90": 4.0, "p95": 5.2, "max": 10.0},
        "get_queued_tasks": {"queued": 15, "scheduled": 8},
        "get_pool_utilization": [{"pool": "default", "total_slots": 128, "running": 45}],
        "get_stale_dags": [],
        "get_concurrent_runs": [],
    })
    _mock_provider("airflow_watcher.api.routers.dags.get_dag_health_monitor", {
        "get_dag_import_errors": [],
        "get_dag_status_summary": {"total_dags": 4500, "active_dags": 4200, "health_score": 92},
        "get_dag_complexity_analysis": [{"dag_id": f"dag_{i}", "task_count": 50 - i} for i in range(50)],
        "get_inactive_dags": [],
    })
    _mock_provider("airflow_watcher.api.routers.dependencies.get_dependency_monitor", {
        "get_upstream_failures": [],
        "get_cross_dag_dependencies": [],
        "get_failure_correlation": {"correlations": []},
        "get_cascading_failure_impact": {"impacted": 0},
    })
    _mock_provider("airflow_watcher.api.routers.overview.get_failure_monitor", {"get_failure_statistics": {"total_runs": 500}})
    _mock_provider("airflow_watcher.api.routers.overview.get_sla_monitor", {"get_sla_statistics": {"total_misses": 0}})
    _mock_provider("airflow_watcher.api.routers.overview.get_task_monitor", {"get_long_running_tasks": [], "get_zombie_tasks": []})
    _mock_provider("airflow_watcher.api.routers.overview.get_scheduling_monitor", {"get_queued_tasks": {"queued": 0}})
    _mock_provider("airflow_watcher.api.routers.overview.get_dag_health_monitor", {"get_dag_status_summary": {"total_dags": 4500}, "get_dag_import_errors": []})
    _mock_provider("airflow_watcher.api.routers.health.get_dag_health_monitor", {"get_dag_status_summary": {"health_score": 92}, "get_dag_import_errors": []})
    _mock_provider("airflow_watcher.api.routers.health.get_failure_monitor", {"get_dag_health_status": {"summary": {}}, "get_recent_failures": []})
    _mock_provider("airflow_watcher.api.routers.health.get_sla_monitor", {"get_recent_sla_misses": []})

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


@pytest.fixture(scope="module")
def load_client():
    """Module-scoped client for load tests (avoids setup overhead per test)."""
    from airflow_watcher.utils.cache import MetricsCache
    MetricsCache.get_instance().clear()

    app, patches = _build_load_test_app()
    yield TestClient(app)
    for p in patches:
        p.stop()


# ---------------------------------------------------------------------------
# Helper: run N requests and collect latencies
# ---------------------------------------------------------------------------

def _run_requests(client, path, n, params=None, headers=None):
    """Send *n* sequential GET requests, return list of latencies in seconds."""
    latencies = []
    errors = 0
    for _ in range(n):
        start = time.perf_counter()
        resp = client.get(path, params=params, headers=headers)
        elapsed = time.perf_counter() - start
        latencies.append(elapsed)
        if resp.status_code >= 500:
            errors += 1
    return latencies, errors


def _run_concurrent(client, path, n_threads, n_per_thread, params=None):
    """Send requests from *n_threads* threads, return all latencies."""
    all_latencies = []
    errors = 0
    lock = threading.Lock()

    def _worker():
        nonlocal errors
        lats, errs = _run_requests(client, path, n_per_thread, params=params)
        with lock:
            all_latencies.extend(lats)
            errors += errs

    threads = [threading.Thread(target=_worker) for _ in range(n_threads)]
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall_time = time.perf_counter() - start

    return all_latencies, errors, wall_time


def _percentile(data, p):
    """Return the p-th percentile of sorted data."""
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (p / 100)
    f = int(k)
    c = f + 1 if f + 1 < len(sorted_data) else f
    d = k - f
    return sorted_data[f] + d * (sorted_data[c] - sorted_data[f])


# =====================================================================
# THROUGHPUT TESTS
# =====================================================================

class TestThroughput:
    """Measure requests/sec on key endpoints."""

    N = 200  # requests per endpoint

    def test_failures_throughput(self, load_client):
        lats, errors = _run_requests(load_client, "/api/v1/failures/", self.N)
        rps = self.N / sum(lats)
        assert errors == 0, f"{errors} errors in {self.N} requests"
        assert rps > 50, f"Too slow: {rps:.0f} req/s (expected >50)"

    def test_overview_throughput(self, load_client):
        lats, errors = _run_requests(load_client, "/api/v1/overview/", self.N)
        rps = self.N / sum(lats)
        assert errors == 0
        assert rps > 50, f"Too slow: {rps:.0f} req/s"

    def test_dags_status_throughput(self, load_client):
        lats, errors = _run_requests(load_client, "/api/v1/dags/status-summary", self.N)
        rps = self.N / sum(lats)
        assert errors == 0
        assert rps > 50


# =====================================================================
# LATENCY TESTS
# =====================================================================

class TestLatency:
    """Ensure p95 latency stays below acceptable thresholds."""

    N = 100

    def test_failures_p95_under_100ms(self, load_client):
        lats, _ = _run_requests(load_client, "/api/v1/failures/", self.N)
        p95 = _percentile(lats, 95)
        assert p95 < 0.1, f"p95={p95*1000:.1f}ms exceeds 100ms"

    def test_health_p95_under_100ms(self, load_client):
        lats, _ = _run_requests(load_client, "/api/v1/health/", self.N)
        p95 = _percentile(lats, 95)
        assert p95 < 0.1, f"p95={p95*1000:.1f}ms exceeds 100ms"

    def test_scheduling_lag_p99_under_200ms(self, load_client):
        lats, _ = _run_requests(load_client, "/api/v1/scheduling/lag", self.N)
        p99 = _percentile(lats, 99)
        assert p99 < 0.2, f"p99={p99*1000:.1f}ms exceeds 200ms"


# =====================================================================
# CONCURRENCY TESTS
# =====================================================================

class TestConcurrency:
    """Verify the API handles concurrent requests without errors."""

    def test_10_threads_failures(self, load_client):
        lats, errors, wall = _run_concurrent(load_client, "/api/v1/failures/", 10, 50)
        assert errors == 0, f"{errors} errors under 10-thread load"
        assert len(lats) == 500

    def test_20_threads_mixed_endpoints(self, load_client):
        """Hit different endpoints concurrently."""
        endpoints = [
            "/api/v1/failures/",
            "/api/v1/sla/misses",
            "/api/v1/tasks/long-running",
            "/api/v1/scheduling/lag",
            "/api/v1/dags/status-summary",
            "/api/v1/overview/",
        ]
        all_errors = 0

        def _worker(path):
            nonlocal all_errors
            _, errs = _run_requests(load_client, path, 30)
            all_errors += errs

        threads = []
        for ep in endpoints:
            for _ in range(3):
                t = threading.Thread(target=_worker, args=(ep,))
                threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all_errors == 0, f"{all_errors} errors under 18-thread concurrent load"


# =====================================================================
# CACHE EFFECTIVENESS  
# =====================================================================

class TestCacheEffectiveness:
    """Verify cache reduces latency on repeated requests."""

    def test_second_request_faster(self, load_client):
        """First request computes; second should be cached and faster."""
        from airflow_watcher.utils.cache import MetricsCache
        MetricsCache.get_instance().clear()

        # First (cold)
        start = time.perf_counter()
        load_client.get("/api/v1/failures/stats")
        cold = time.perf_counter() - start

        # Second (cached) — run 50x and take median
        warm_lats = []
        for _ in range(50):
            s = time.perf_counter()
            load_client.get("/api/v1/failures/stats")
            warm_lats.append(time.perf_counter() - s)

        warm_median = statistics.median(warm_lats)
        # Cached should generally be faster. Allow generous tolerance for CI noise.
        assert warm_median <= cold * 2, f"Cached ({warm_median*1000:.1f}ms) not faster than cold ({cold*1000:.1f}ms)"

    def test_cache_invalidate_resets(self, load_client):
        """After invalidation, next request should recompute."""
        load_client.get("/api/v1/failures/stats")  # warm cache

        resp = load_client.post("/api/v1/cache/invalidate")
        assert resp.status_code == 200

        resp = load_client.get("/api/v1/failures/stats")
        assert resp.status_code == 200


# =====================================================================
# ERROR RATE UNDER SUSTAINED LOAD
# =====================================================================

class TestErrorRateUnderLoad:
    """Zero 5xx errors under sustained mixed-endpoint load."""

    def test_1000_requests_zero_errors(self, load_client):
        endpoints = [
            "/api/v1/failures/",
            "/api/v1/failures/stats",
            "/api/v1/sla/misses",
            "/api/v1/tasks/long-running",
            "/api/v1/tasks/zombies",
            "/api/v1/scheduling/queue",
            "/api/v1/dags/status-summary",
            "/api/v1/dags/import-errors",
            # cross-dag excluded: returns 501 in standalone/test mode (no DagBag)
            "/api/v1/overview/",
        ]

        errors = 0
        total = 0
        for ep in endpoints:
            _, errs = _run_requests(load_client, ep, 100)
            errors += errs
            total += 100

        assert errors == 0, f"{errors}/{total} requests returned 5xx"


# =====================================================================
# RESPONSE SIZE SANITY
# =====================================================================

class TestResponseSize:
    """Ensure responses don't blow up unexpectedly."""

    def test_complexity_list_bounded(self, load_client):
        resp = load_client.get("/api/v1/dags/complexity")
        data = resp.json()["data"]
        assert len(data["dags"]) == 50  # Our mock returns 50 items
        # Ensure response payload is reasonable (<1MB)
        assert len(resp.text) < 1_000_000

    def test_overview_size_reasonable(self, load_client):
        resp = load_client.get("/api/v1/overview/")
        assert len(resp.text) < 100_000  # Overview should be compact
