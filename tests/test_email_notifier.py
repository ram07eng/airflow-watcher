"""Tests for Email Notifier."""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from airflow_watcher.notifiers.email_notifier import EmailNotifier
from airflow_watcher.models.failure import DAGFailure, TaskFailure
from airflow_watcher.models.sla import SLAMissEvent
from airflow_watcher.config import WatcherConfig


class TestEmailNotifier:
    """Test cases for EmailNotifier."""

    def test_init_with_default_config(self):
        """Test initialization with default config."""
        notifier = EmailNotifier()
        assert notifier.config is not None

    def test_init_with_custom_config(self):
        """Test initialization with custom config."""
        config = WatcherConfig(
            smtp_host="smtp.example.com",
            smtp_port=587,
            email_recipients=["test@example.com"],
        )
        notifier = EmailNotifier(config=config)
        assert notifier.config.smtp_host == "smtp.example.com"

    def test_build_failure_html(self):
        """Test building HTML content for failure alert."""
        notifier = EmailNotifier()
        now = datetime.utcnow()
        
        failure = DAGFailure(
            dag_id="test_dag",
            run_id="manual__2024-01-01",
            execution_date=now,
            start_date=now,
            end_date=now,
            failed_tasks=[
                TaskFailure(
                    task_id="task1",
                    dag_id="test_dag",
                    run_id="manual__2024-01-01",
                    execution_date=now,
                    try_number=1,
                    max_tries=3,
                ),
            ],
        )
        
        html = notifier._build_failure_html(failure)
        
        assert "test_dag" in html
        assert "DAG Failure Alert" in html
        assert "task1" in html

    def test_build_sla_miss_html(self):
        """Test building HTML content for SLA miss alert."""
        notifier = EmailNotifier()
        now = datetime.utcnow()
        
        sla_miss = SLAMissEvent(
            dag_id="test_dag",
            task_id="test_task",
            execution_date=now,
            timestamp=now,
            description="Missed deadline",
        )
        
        html = notifier._build_sla_miss_html(sla_miss)
        
        assert "test_dag" in html
        assert "test_task" in html
        assert "SLA Miss Alert" in html

    def test_send_failure_alert_no_recipients(self):
        """Test failure alert with no recipients."""
        notifier = EmailNotifier()
        
        now = datetime.utcnow()
        failure = DAGFailure(
            dag_id="test_dag",
            run_id="test_run",
            execution_date=now,
        )
        
        result = notifier.send_failure_alert(failure)
        assert result is False

    def test_send_failure_alert_no_smtp(self):
        """Test failure alert with no SMTP configured."""
        config = WatcherConfig(
            email_recipients=["test@example.com"],
        )
        notifier = EmailNotifier(config=config)
        
        now = datetime.utcnow()
        failure = DAGFailure(
            dag_id="test_dag",
            run_id="test_run",
            execution_date=now,
        )
        
        result = notifier.send_failure_alert(failure)
        assert result is False

    @patch("smtplib.SMTP")
    def test_send_email_success(self, mock_smtp):
        """Test successful email sending."""
        config = WatcherConfig(
            smtp_host="smtp.example.com",
            smtp_port=587,
            email_recipients=["test@example.com"],
        )
        notifier = EmailNotifier(config=config)
        
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server
        
        result = notifier._send_email(
            subject="Test Subject",
            html_body="<p>Test body</p>",
            recipients=["test@example.com"],
        )
        
        assert result is True

    def test_build_summary_html(self):
        """Test building daily summary HTML."""
        notifier = EmailNotifier()
        now = datetime.utcnow()
        
        failures = [
            DAGFailure(
                dag_id=f"dag_{i}",
                run_id=f"run_{i}",
                execution_date=now,
                failed_tasks=[],
            )
            for i in range(3)
        ]
        
        sla_misses = [
            SLAMissEvent(
                dag_id=f"dag_{i}",
                task_id=f"task_{i}",
                execution_date=now,
                timestamp=now,
            )
            for i in range(2)
        ]
        
        html = notifier._build_summary_html(failures, sla_misses)
        
        assert "Daily Summary" in html
        assert "dag_0" in html
        assert "DAG Failures (3)" in html
        assert "SLA Misses (2)" in html
