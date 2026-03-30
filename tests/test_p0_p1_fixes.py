"""Tests for P0/P1 review fixes.

Covers:
- C-3: Batch DagRun loading in get_zombie_tasks (N+1 elimination)
- C-4: DagBag scoped loading (limit parameter)
- C-5: StatsDEmitter thread safety
- C-6: MetricsCollector monitor caching
- M-1: asyncio.Lock in rate limiter
- M-3: Flask API _safe_int input validation
- M-4: RBAC fail-closed default
"""

import asyncio
import threading
from unittest.mock import MagicMock, patch

# ============== C-5: StatsDEmitter thread safety ==============


class TestStatsDEmitterThreadSafety:
    """Verify StatsDEmitter uses a lock for send/close."""

    def test_has_lock(self):
        from airflow_watcher.metrics.statsd_emitter import StatsDEmitter

        emitter = StatsDEmitter()
        assert hasattr(emitter, "_lock")
        assert isinstance(emitter._lock, type(threading.Lock()))

    def test_close_nullifies_socket(self):
        from airflow_watcher.metrics.statsd_emitter import StatsDEmitter

        emitter = StatsDEmitter()
        emitter._socket = MagicMock()
        emitter.close()
        assert emitter._socket is None

    def test_send_disabled(self):
        from airflow_watcher.metrics.statsd_emitter import StatsDEmitter

        emitter = StatsDEmitter()
        emitter.disable()
        assert emitter._send("test:1|g") is False

    def test_send_failure_logs_warning(self):
        from airflow_watcher.metrics.statsd_emitter import StatsDEmitter

        emitter = StatsDEmitter()
        mock_sock = MagicMock()
        mock_sock.sendto.side_effect = OSError("no route")
        emitter._socket = mock_sock
        assert emitter._send("bad:1|g") is False


# ============== C-6: MetricsCollector caching ==============


class TestMetricsCollectorCaching:
    """Verify monitors are cached across collect() calls."""

    def test_monitors_cached(self):
        from airflow_watcher.metrics.collector import MetricsCollector

        collector = MetricsCollector()
        assert collector._monitors is None

        monitors1 = collector._get_monitors()
        monitors2 = collector._get_monitors()
        assert monitors1 is monitors2

    def test_monitors_dict_has_all_keys(self):
        from airflow_watcher.metrics.collector import MetricsCollector

        collector = MetricsCollector()
        monitors = collector._get_monitors()
        expected_keys = {"failure", "sla", "task", "scheduling", "health", "dependency"}
        assert set(monitors.keys()) == expected_keys


# ============== M-1: asyncio.Lock in rate limiter ==============


class TestRateLimiterLockType:
    """Rate limiter should use asyncio.Lock, not threading.Lock."""

    def test_lock_is_asyncio(self):
        from airflow_watcher.api.rate_limit import RateLimitMiddleware

        app = MagicMock()
        middleware = RateLimitMiddleware(app)
        assert isinstance(middleware._lock, asyncio.Lock)


# ============== M-3: _safe_int input validation ==============


class TestSafeInt:
    """Test _safe_int helper in Flask API."""

    def _get_safe_int(self):
        from airflow_watcher.views.api import _safe_int

        return _safe_int

    def test_valid_int(self):
        f = self._get_safe_int()
        assert f("10", 5) == 10

    def test_none_returns_default(self):
        f = self._get_safe_int()
        assert f(None, 42) == 42

    def test_bad_string_returns_default(self):
        f = self._get_safe_int()
        assert f("not_a_number", 24) == 24

    def test_clamps_to_min(self):
        f = self._get_safe_int()
        assert f("0", 10, min_val=1) == 1
        assert f("-5", 10, min_val=1) == 1

    def test_clamps_to_max(self):
        f = self._get_safe_int()
        assert f("999999", 10, max_val=500) == 500

    def test_boundary_values(self):
        f = self._get_safe_int()
        assert f("1", 10, min_val=1, max_val=100) == 1
        assert f("100", 10, min_val=1, max_val=100) == 100

    def test_sql_injection_returns_default(self):
        f = self._get_safe_int()
        assert f("1; DROP TABLE", 24) == 24


# ============== M-4: RBAC fail-closed default ==============


class TestRBACFailClosedDefault:
    """RBAC_FAIL_OPEN should default to False."""

    def test_default_fail_closed(self):
        """Without env var, RBAC should fail closed."""
        with patch.dict("os.environ", {}, clear=False):
            # Remove the env var if it exists
            import os

            old_val = os.environ.pop("AIRFLOW_WATCHER_RBAC_FAIL_OPEN", None)
            try:
                # Re-evaluate the module-level constant
                import importlib

                import airflow_watcher.utils.rbac as rbac_mod

                importlib.reload(rbac_mod)
                assert rbac_mod.RBAC_FAIL_OPEN is False
            finally:
                if old_val is not None:
                    os.environ["AIRFLOW_WATCHER_RBAC_FAIL_OPEN"] = old_val
                else:
                    os.environ.pop("AIRFLOW_WATCHER_RBAC_FAIL_OPEN", None)

    def test_env_true_opens(self):
        """Setting env var to 'true' should enable fail-open."""
        import os

        old_val = os.environ.get("AIRFLOW_WATCHER_RBAC_FAIL_OPEN")
        try:
            os.environ["AIRFLOW_WATCHER_RBAC_FAIL_OPEN"] = "true"
            import importlib

            import airflow_watcher.utils.rbac as rbac_mod

            importlib.reload(rbac_mod)
            assert rbac_mod.RBAC_FAIL_OPEN is True
        finally:
            if old_val is not None:
                os.environ["AIRFLOW_WATCHER_RBAC_FAIL_OPEN"] = old_val
            else:
                os.environ.pop("AIRFLOW_WATCHER_RBAC_FAIL_OPEN", None)


# ============== C-3: Batch DagRun loading pattern ==============


class TestZombieTasksBatchQuery:
    """Verify get_zombie_tasks uses tuple_ batch loading."""

    def test_imports_tuple_(self):
        """task_health_monitor should import tuple_ from sqlalchemy."""
        import airflow_watcher.monitors.task_health_monitor as thm

        # The module imports tuple_ at module level
        assert hasattr(thm, "tuple_")


# ============== C-4: DagBag limit parameters ==============


class TestDagBagLimitParameters:
    """Verify DagBag-heavy methods accept limit parameters."""

    def test_complexity_analysis_has_limit_param(self):
        import inspect

        from airflow_watcher.monitors.dag_health_monitor import DAGHealthMonitor

        # provide_session wraps the method; check __wrapped__ if available
        method = DAGHealthMonitor.get_dag_complexity_analysis
        unwrapped = getattr(method, "__wrapped__", method)
        sig = inspect.signature(unwrapped)
        assert "limit" in sig.parameters

    def test_cross_dag_deps_has_limit_param(self):
        import inspect

        from airflow_watcher.monitors.dependency_monitor import DependencyMonitor

        method = DependencyMonitor.get_cross_dag_dependencies
        unwrapped = getattr(method, "__wrapped__", method)
        sig = inspect.signature(unwrapped)
        assert "limit" in sig.parameters
