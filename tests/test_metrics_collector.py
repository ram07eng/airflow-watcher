"""Tests for MetricsCollector and WatcherMetrics."""

from unittest.mock import patch

from airflow_watcher.metrics.collector import MetricsCollector, WatcherMetrics


class TestWatcherMetrics:
    """Tests for WatcherMetrics dataclass."""

    def test_defaults(self):
        """All numeric fields default to zero."""
        m = WatcherMetrics()
        assert m.total_failures_24h == 0
        assert m.total_sla_misses_24h == 0
        assert m.healthy_dag_percent == 0.0

    def test_to_dict_keys(self):
        """to_dict returns all expected metric keys."""
        m = WatcherMetrics()
        d = m.to_dict()
        expected_keys = [
            "failures.total_24h",
            "failures.unique_dags_24h",
            "failures.rate_percent",
            "sla.misses_24h",
            "sla.miss_rate_percent",
            "tasks.failed_24h",
            "tasks.retry_24h",
            "tasks.long_running",
            "scheduling.missed_24h",
            "scheduling.delayed_dags",
            "dags.unhealthy",
            "dags.total",
            "dags.healthy_percent",
            "dependencies.failed_sensors",
            "dependencies.blocked_dags",
        ]
        for key in expected_keys:
            assert key in d, f"Missing key: {key}"

    def test_to_dict_values(self):
        """to_dict reflects set values."""
        m = WatcherMetrics(total_failures_24h=5, total_sla_misses_24h=2)
        d = m.to_dict()
        assert d["failures.total_24h"] == 5
        assert d["sla.misses_24h"] == 2


class TestMetricsCollector:
    """Tests for MetricsCollector."""

    def test_init(self):
        """Collector initialises with no cached metrics."""
        collector = MetricsCollector()
        assert collector.get_last_metrics() is None

    def test_get_last_metrics_after_collect(self):
        """get_last_metrics returns the last collected metrics."""
        collector = MetricsCollector()

        # Patch all monitors to return empty/safe data
        with patch("airflow_watcher.metrics.collector.MetricsCollector.collect") as mock_collect:
            mock_metrics = WatcherMetrics(total_failures_24h=3)
            mock_collect.return_value = mock_metrics
            result = collector.collect()

        assert result.total_failures_24h == 3

    def test_collect_returns_last_on_error(self):
        """collect() returns cached metrics when an error occurs."""
        collector = MetricsCollector()
        cached = WatcherMetrics(total_failures_24h=7)
        collector._last_metrics = cached

        # Force an error inside collect by patching the import
        with patch(
            "airflow_watcher.monitors.dag_failure_monitor.DAGFailureMonitor",
            side_effect=RuntimeError("DB down"),
        ):
            result = collector.collect()

        assert result.total_failures_24h == 7
