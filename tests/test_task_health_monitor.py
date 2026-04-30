"""Tests for TaskHealthMonitor."""

from airflow_watcher.monitors.task_health_monitor import TaskHealthMonitor


class TestTaskHealthMonitor:
    """Tests for TaskHealthMonitor."""

    def test_init(self):
        monitor = TaskHealthMonitor()
        assert monitor.config is not None

    def test_init_with_custom_config(self):
        from airflow_watcher.config import WatcherConfig

        config = WatcherConfig(failure_lookback_hours=12)
        monitor = TaskHealthMonitor(config=config)
        assert monitor.config is config

    def test_monitor_has_required_methods(self):
        """TaskHealthMonitor exposes expected public API."""
        monitor = TaskHealthMonitor()
        assert callable(getattr(monitor, "get_long_running_tasks", None))
        assert callable(getattr(monitor, "get_retry_heavy_tasks", None))
        assert callable(getattr(monitor, "get_zombie_tasks", None))
        assert callable(getattr(monitor, "get_task_failure_patterns", None))

    # ------------------------------------------------------------------
    # _get_duration_severity — pure logic
    # ------------------------------------------------------------------

    def test_severity_low_just_above_threshold(self):
        monitor = TaskHealthMonitor()
        # ratio = 1.0 / 1 = 1.0  → "low" (< 1.5)
        assert monitor._get_duration_severity(60.0, 60) == "low"

    def test_severity_medium(self):
        monitor = TaskHealthMonitor()
        # ratio = 90/60 = 1.5 → "medium"
        assert monitor._get_duration_severity(90.0, 60) == "medium"

    def test_severity_high(self):
        monitor = TaskHealthMonitor()
        # ratio = 120/60 = 2.0 → "high"
        assert monitor._get_duration_severity(120.0, 60) == "high"

    def test_severity_critical(self):
        monitor = TaskHealthMonitor()
        # ratio = 180/60 = 3.0 → "critical"
        assert monitor._get_duration_severity(180.0, 60) == "critical"

    def test_severity_critical_far_above(self):
        monitor = TaskHealthMonitor()
        # ratio = 600/60 = 10 → "critical"
        assert monitor._get_duration_severity(600.0, 60) == "critical"

    def test_severity_boundaries_are_monotone(self):
        """Higher ratios always yield equal-or-higher severity rank."""
        monitor = TaskHealthMonitor()
        rank = {"low": 0, "medium": 1, "high": 2, "critical": 3}
        threshold = 60
        prev_rank = -1
        for running in [61, 90, 120, 180, 240]:
            s = monitor._get_duration_severity(float(running), threshold)
            assert rank[s] >= prev_rank
            prev_rank = rank[s]

    def test_severity_different_thresholds(self):
        """Severity is relative to threshold, not absolute minutes."""
        monitor = TaskHealthMonitor()
        # 30 min running against threshold=10 → ratio=3 → critical
        assert monitor._get_duration_severity(30.0, 10) == "critical"
        # 30 min running against threshold=60 → ratio=0.5 → low
        assert monitor._get_duration_severity(30.0, 60) == "low"
