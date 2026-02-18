"""Tests for SLA Monitor."""

from datetime import datetime, timedelta

from airflow_watcher.models.sla import SLADelayWarning, SLAMissEvent
from airflow_watcher.monitors.sla_monitor import SLAMonitor


class TestSLAMonitor:
    """Test cases for SLAMonitor."""

    def test_init_with_default_config(self):
        """Test initialization with default config."""
        monitor = SLAMonitor()
        assert monitor.config is not None

    def test_init_with_custom_config(self):
        """Test initialization with custom config."""
        from airflow_watcher.config import WatcherConfig

        config = WatcherConfig(sla_check_interval_minutes=10)
        monitor = SLAMonitor(config=config)
        assert monitor.config.sla_check_interval_minutes == 10

    def test_sla_miss_event_to_dict(self):
        """Test SLAMissEvent serialization."""
        now = datetime.utcnow()
        event = SLAMissEvent(
            dag_id="test_dag",
            task_id="test_task",
            execution_date=now,
            timestamp=now,
            email_sent=True,
            notification_sent=True,
            description="Task missed SLA by 30 minutes",
        )

        result = event.to_dict()

        assert result["dag_id"] == "test_dag"
        assert result["task_id"] == "test_task"
        assert result["email_sent"] is True
        assert result["notification_sent"] is True
        assert result["description"] == "Task missed SLA by 30 minutes"

    def test_sla_delay_warning_to_dict(self):
        """Test SLADelayWarning serialization."""
        now = datetime.utcnow()
        warning = SLADelayWarning(
            dag_id="test_dag",
            run_id="test_run",
            execution_date=now,
            start_date=now - timedelta(minutes=45),
            running_minutes=45.0,
            threshold_minutes=30.0,
            risk_level="high",
        )

        result = warning.to_dict()

        assert result["dag_id"] == "test_dag"
        assert result["running_minutes"] == 45.0
        assert result["threshold_minutes"] == 30.0
        assert result["risk_level"] == "high"

    def test_sla_miss_event_minimal(self):
        """Test SLAMissEvent with minimal data."""
        now = datetime.utcnow()
        event = SLAMissEvent(
            dag_id="test_dag",
            task_id="test_task",
            execution_date=now,
            timestamp=now,
        )

        result = event.to_dict()

        assert result["email_sent"] is False
        assert result["notification_sent"] is False
        assert result["description"] is None
