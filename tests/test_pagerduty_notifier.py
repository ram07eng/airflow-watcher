"""Tests for PagerDuty Notifier."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from airflow_watcher.config import WatcherConfig
from airflow_watcher.models.failure import DAGFailure, TaskFailure
from airflow_watcher.models.sla import SLAMissEvent
from airflow_watcher.notifiers.pagerduty_notifier import PagerDutyNotifier


class TestPagerDutyNotifier:
    """Tests for PagerDutyNotifier."""

    def test_init_with_config(self):
        config = WatcherConfig(pagerduty_routing_key="test-key")
        notifier = PagerDutyNotifier(config=config)
        assert notifier.routing_key == "test-key"

    def test_generate_dedup_key_deterministic(self):
        config = WatcherConfig(pagerduty_routing_key="key")
        notifier = PagerDutyNotifier(config=config)
        key1 = notifier._generate_dedup_key("dag_failure", "my_dag")
        key2 = notifier._generate_dedup_key("dag_failure", "my_dag")
        assert key1 == key2

    def test_generate_dedup_key_unique(self):
        config = WatcherConfig(pagerduty_routing_key="key")
        notifier = PagerDutyNotifier(config=config)
        key1 = notifier._generate_dedup_key("dag_failure", "dag_a")
        key2 = notifier._generate_dedup_key("dag_failure", "dag_b")
        assert key1 != key2

    def test_send_event_no_routing_key(self):
        """Returns False when routing key is not set."""
        config = WatcherConfig()
        notifier = PagerDutyNotifier(config=config)
        result = notifier._send_event("summary", "error", "src", "key")
        assert result is False

    @patch("airflow_watcher.notifiers.pagerduty_notifier.requests.post")
    def test_send_event_success(self, mock_post):
        mock_post.return_value.status_code = 202
        config = WatcherConfig(pagerduty_routing_key="test-key")
        notifier = PagerDutyNotifier(config=config)
        result = notifier._send_event("summary", "error", "src", "dedup-key")
        assert result is True

    @patch("airflow_watcher.notifiers.pagerduty_notifier.requests.post")
    def test_send_event_api_error(self, mock_post):
        mock_post.return_value.status_code = 400
        mock_post.return_value.text = "Bad Request"
        config = WatcherConfig(pagerduty_routing_key="test-key")
        notifier = PagerDutyNotifier(config=config)
        result = notifier._send_event("summary", "error", "src", "dedup-key")
        assert result is False

    @patch("airflow_watcher.notifiers.pagerduty_notifier.requests.post")
    def test_send_failure_alert(self, mock_post):
        mock_post.return_value.status_code = 202
        config = WatcherConfig(pagerduty_routing_key="test-key")
        notifier = PagerDutyNotifier(config=config)

        now = datetime.utcnow()
        failure = DAGFailure(
            dag_id="test_dag",
            run_id="run_1",
            execution_date=now,
            failed_tasks=[
                TaskFailure(task_id="t1", dag_id="test_dag", run_id="run_1", execution_date=now)
            ],
        )
        result = notifier.send_failure_alert(failure)
        assert result is True
        # Verify the payload contains dag_id
        call_kwargs = mock_post.call_args[1]
        assert call_kwargs["json"]["payload"]["custom_details"]["dag_id"] == "test_dag"

    @patch("airflow_watcher.notifiers.pagerduty_notifier.requests.post")
    def test_send_sla_miss_alert(self, mock_post):
        mock_post.return_value.status_code = 202
        config = WatcherConfig(pagerduty_routing_key="test-key")
        notifier = PagerDutyNotifier(config=config)

        now = datetime.utcnow()
        sla_miss = SLAMissEvent(
            dag_id="test_dag",
            task_id="test_task",
            execution_date=now,
            timestamp=now,
        )
        result = notifier.send_sla_miss_alert(sla_miss)
        assert result is True

    def test_send_batch_alert_empty(self):
        config = WatcherConfig(pagerduty_routing_key="test-key")
        notifier = PagerDutyNotifier(config=config)
        result = notifier.send_batch_alert([])
        assert result is True
