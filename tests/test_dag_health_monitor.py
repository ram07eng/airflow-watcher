"""Tests for DAGHealthMonitor."""

from airflow_watcher.monitors.dag_health_monitor import DAGHealthMonitor


class TestDAGHealthMonitor:
    """Tests for DAGHealthMonitor."""

    def test_init(self):
        monitor = DAGHealthMonitor()
        assert monitor.config is not None

    # ------------------------------------------------------------------
    # _calculate_health_score
    # ------------------------------------------------------------------

    def test_health_score_no_dags(self):
        """Returns 100 when there are no DAGs."""
        monitor = DAGHealthMonitor()
        assert monitor._calculate_health_score(0, 0, 0) == 100

    def test_health_score_perfect(self):
        """Returns 100 with no errors or failures."""
        monitor = DAGHealthMonitor()
        assert monitor._calculate_health_score(10, 0, 0) == 100

    def test_health_score_with_errors(self):
        """Score decreases with import errors."""
        monitor = DAGHealthMonitor()
        score_no_errors = monitor._calculate_health_score(10, 0, 0)
        score_with_errors = monitor._calculate_health_score(10, 2, 0)
        assert score_with_errors < score_no_errors

    def test_health_score_with_failures(self):
        """Score decreases with failures."""
        monitor = DAGHealthMonitor()
        score_no_failures = monitor._calculate_health_score(10, 0, 0)
        score_with_failures = monitor._calculate_health_score(10, 0, 5)
        assert score_with_failures < score_no_failures

    def test_health_score_never_negative(self):
        """Score is always >= 0."""
        monitor = DAGHealthMonitor()
        score = monitor._calculate_health_score(10, 100, 100)
        assert score >= 0

    def test_health_score_custom_weights(self):
        """Custom weights are applied correctly."""
        monitor = DAGHealthMonitor()
        # With a very high error_weight, errors should dominate
        score = monitor._calculate_health_score(
            10, 5, 0, weights={"error_weight": 100, "error_cap": 90, "failure_weight": 1, "failure_cap": 40}
        )
        assert score <= 10

    def test_monitor_has_required_methods(self):
        """DAGHealthMonitor exposes the expected public API."""
        monitor = DAGHealthMonitor()
        assert callable(getattr(monitor, "get_dag_import_errors", None))
        assert callable(getattr(monitor, "get_dag_status_summary", None))
        assert callable(getattr(monitor, "get_dag_complexity_analysis", None))
        assert callable(getattr(monitor, "get_inactive_dags", None))

    def test_health_score_boundary_all_errors(self):
        """Health score caps error deduction correctly."""
        monitor = DAGHealthMonitor()
        # 10 errors out of 10 DAGs - should hit the error_cap
        score = monitor._calculate_health_score(10, 10, 0)
        assert 0 <= score <= 100
