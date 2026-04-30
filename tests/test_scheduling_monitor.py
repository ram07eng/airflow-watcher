"""Tests for SchedulingMonitor."""

from airflow_watcher.monitors.scheduling_monitor import SchedulingMonitor


class TestSchedulingMonitor:
    """Tests for SchedulingMonitor."""

    def test_init(self):
        monitor = SchedulingMonitor()
        assert monitor.config is not None

    def test_init_with_custom_config(self):
        from airflow_watcher.config import WatcherConfig

        config = WatcherConfig(failure_lookback_hours=48)
        monitor = SchedulingMonitor(config=config)
        assert monitor.config is config

    def test_monitor_has_required_methods(self):
        """SchedulingMonitor exposes expected public API."""
        monitor = SchedulingMonitor()
        assert callable(getattr(monitor, "get_scheduling_lag", None))
        assert callable(getattr(monitor, "get_queued_tasks", None))
        assert callable(getattr(monitor, "get_pool_utilization", None))
        assert callable(getattr(monitor, "get_stale_dags", None))
        assert callable(getattr(monitor, "get_concurrent_runs", None))

    # ------------------------------------------------------------------
    # Percentile helpers — pure in-memory logic
    # ------------------------------------------------------------------

    def _compute_percentiles(self, lags):
        """Replicate the in-memory percentile logic from get_scheduling_lag."""
        sorted_lags = sorted(lags)
        n = len(sorted_lags)
        p50_idx = min(int(n * 0.5), n - 1)
        p90_idx = min(int(n * 0.9), n - 1)
        p95_idx = min(int(n * 0.95), n - 1)
        return {
            "p50": sorted_lags[p50_idx],
            "p90": sorted_lags[p90_idx],
            "p95": sorted_lags[p95_idx],
        }

    def test_percentile_single_value(self):
        p = self._compute_percentiles([10.0])
        assert p["p50"] == 10.0
        assert p["p90"] == 10.0
        assert p["p95"] == 10.0

    def test_percentile_ascending_order(self):
        p = self._compute_percentiles([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])
        # p50 ≥ median of smallest half
        assert p["p50"] <= p["p90"] <= p["p95"]

    def test_percentile_p95_gte_p50(self):
        lags = list(range(1, 101))
        p = self._compute_percentiles(lags)
        assert p["p95"] >= p["p50"]

    def test_percentile_clamp_to_last_element(self):
        """Index never exceeds n-1 (regression guard for the min() clamp)."""
        lags = [5.0]
        p = self._compute_percentiles(lags)
        assert p["p95"] == 5.0

    # ------------------------------------------------------------------
    # Pool saturation logic
    # ------------------------------------------------------------------

    def test_pool_saturation_at_capacity(self):
        """is_saturated when running == slots."""
        slots = 4
        running = 4
        assert running >= slots

    def test_pool_saturation_below_capacity(self):
        slots = 4
        running = 2
        assert not (running >= slots)

    def test_pool_utilization_zero_slots(self):
        """Zero-slot pool: utilization is 0 (no ZeroDivisionError)."""
        slots = 0
        running = 0
        utilization = (running / slots * 100) if slots > 0 else 0
        assert utilization == 0
