"""Tests for DAG Failure Monitor."""

from datetime import datetime, timedelta

from airflow_watcher.models.failure import DAGFailure, TaskFailure
from airflow_watcher.monitors.dag_failure_monitor import DAGFailureMonitor


class TestDAGFailureMonitor:
    """Test cases for DAGFailureMonitor."""

    def test_init_with_default_config(self):
        """Test initialization with default config."""
        monitor = DAGFailureMonitor()
        assert monitor.config is not None

    def test_init_with_custom_config(self):
        """Test initialization with custom config."""
        from airflow_watcher.config import WatcherConfig

        config = WatcherConfig(failure_lookback_hours=48)
        monitor = DAGFailureMonitor(config=config)
        assert monitor.config.failure_lookback_hours == 48

    def test_dag_failure_to_dict(self):
        """Test DAGFailure serialization."""
        now = datetime.utcnow()
        failure = DAGFailure(
            dag_id="test_dag",
            run_id="manual__2024-01-01",
            execution_date=now,
            start_date=now - timedelta(hours=1),
            end_date=now,
            state="failed",
            failed_tasks=[
                TaskFailure(
                    task_id="task1",
                    dag_id="test_dag",
                    run_id="manual__2024-01-01",
                    execution_date=now,
                )
            ],
        )

        result = failure.to_dict()

        assert result["dag_id"] == "test_dag"
        assert result["state"] == "failed"
        assert result["failed_task_count"] == 1
        assert len(result["failed_tasks"]) == 1

    def test_task_failure_to_dict(self):
        """Test TaskFailure serialization."""
        now = datetime.utcnow()
        task_failure = TaskFailure(
            task_id="test_task",
            dag_id="test_dag",
            run_id="manual__2024-01-01",
            execution_date=now,
            try_number=2,
            max_tries=3,
        )

        result = task_failure.to_dict()

        assert result["task_id"] == "test_task"
        assert result["try_number"] == 2
        assert result["max_tries"] == 3

    def test_dag_failure_duration(self):
        """Test DAGFailure duration calculation."""
        now = datetime.utcnow()
        failure = DAGFailure(
            dag_id="test_dag",
            run_id="test_run",
            execution_date=now,
            start_date=now - timedelta(seconds=3600),
            end_date=now,
        )

        assert failure.duration == 3600

    def test_dag_failure_duration_none(self):
        """Test DAGFailure duration when dates missing."""
        failure = DAGFailure(
            dag_id="test_dag",
            run_id="test_run",
            execution_date=datetime.utcnow(),
        )

        assert failure.duration is None

    def test_dag_failure_external_trigger_default(self):
        """Test DAGFailure external_trigger defaults to False."""
        failure = DAGFailure(
            dag_id="test_dag",
            run_id="test_run",
            execution_date=datetime.utcnow(),
        )
        assert failure.external_trigger is False

    def test_dag_failure_no_failed_tasks(self):
        """Test DAGFailure with no failed tasks."""
        failure = DAGFailure(
            dag_id="test_dag",
            run_id="test_run",
            execution_date=datetime.utcnow(),
        )
        result = failure.to_dict()
        assert result["failed_task_count"] == 0
        assert result["failed_tasks"] == []

    def test_monitor_has_required_methods(self):
        """DAGFailureMonitor exposes the expected public API."""
        monitor = DAGFailureMonitor()
        assert callable(getattr(monitor, "get_recent_failures", None))
        assert callable(getattr(monitor, "get_failure_statistics", None))
        assert callable(getattr(monitor, "get_dag_health_status", None))

    def test_failure_statistics_config_default(self):
        """DAGFailureMonitor uses config lookback_hours default of 24."""
        monitor = DAGFailureMonitor()
        assert monitor.config.failure_lookback_hours == 24
