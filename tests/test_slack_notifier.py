"""Tests for Slack Notifier."""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from airflow_watcher.notifiers.slack_notifier import SlackNotifier
from airflow_watcher.models.failure import DAGFailure, TaskFailure
from airflow_watcher.models.sla import SLAMissEvent
from airflow_watcher.config import WatcherConfig


class TestSlackNotifier:
    """Test cases for SlackNotifier."""

    def test_init_with_default_config(self):
        """Test initialization with default config."""
        notifier = SlackNotifier()
        assert notifier.config is not None
        assert notifier.webhook_client is None
        assert notifier.web_client is None

    def test_init_with_webhook_url(self):
        """Test initialization with webhook URL."""
        config = WatcherConfig(
            slack_webhook_url="https://hooks.slack.com/services/xxx/yyy/zzz"
        )
        notifier = SlackNotifier(config=config)
        assert notifier.webhook_client is not None

    def test_build_failure_blocks(self):
        """Test building Slack blocks for failure alert."""
        notifier = SlackNotifier()
        now = datetime.utcnow()
        
        failure = DAGFailure(
            dag_id="test_dag",
            run_id="manual__2024-01-01",
            execution_date=now,
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
        
        blocks = notifier._build_failure_blocks(failure)
        
        assert len(blocks) > 0
        assert blocks[0]["type"] == "header"
        assert "DAG Failure" in blocks[0]["text"]["text"]

    def test_build_sla_miss_blocks(self):
        """Test building Slack blocks for SLA miss alert."""
        notifier = SlackNotifier()
        now = datetime.utcnow()
        
        sla_miss = SLAMissEvent(
            dag_id="test_dag",
            task_id="test_task",
            execution_date=now,
            timestamp=now,
            description="Missed by 15 minutes",
        )
        
        blocks = notifier._build_sla_miss_blocks(sla_miss)
        
        assert len(blocks) > 0
        assert blocks[0]["type"] == "header"
        assert "SLA Miss" in blocks[0]["text"]["text"]

    @patch("airflow_watcher.notifiers.slack_notifier.WebhookClient")
    def test_send_failure_alert_success(self, mock_webhook_client):
        """Test successful failure alert sending."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_webhook_client.return_value.send.return_value = mock_response
        
        config = WatcherConfig(
            slack_webhook_url="https://hooks.slack.com/services/xxx"
        )
        notifier = SlackNotifier(config=config)
        notifier.webhook_client = mock_webhook_client.return_value
        
        now = datetime.utcnow()
        failure = DAGFailure(
            dag_id="test_dag",
            run_id="test_run",
            execution_date=now,
        )
        
        result = notifier.send_failure_alert(failure)
        assert result is True

    def test_send_failure_alert_no_client(self):
        """Test failure alert when no client configured."""
        notifier = SlackNotifier()
        
        now = datetime.utcnow()
        failure = DAGFailure(
            dag_id="test_dag",
            run_id="test_run",
            execution_date=now,
        )
        
        result = notifier.send_failure_alert(failure)
        assert result is False
