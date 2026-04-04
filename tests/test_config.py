"""Tests for WatcherConfig."""

import os
from unittest.mock import patch

from airflow_watcher.config import WatcherConfig


class TestWatcherConfig:
    """Test cases for WatcherConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = WatcherConfig()

        assert config.slack_webhook_url is None
        assert config.slack_channel == "#airflow-alerts"
        assert config.smtp_port == 587
        assert config.failure_lookback_hours == 24
        assert config.sla_check_interval_minutes == 5

    def test_custom_config(self):
        """Test custom configuration values."""
        config = WatcherConfig(
            slack_webhook_url="https://hooks.slack.com/test",
            slack_channel="#test-channel",
            failure_lookback_hours=48,
        )

        assert config.slack_webhook_url == "https://hooks.slack.com/test"
        assert config.slack_channel == "#test-channel"
        assert config.failure_lookback_hours == 48

    def test_email_recipients_list(self):
        """Test email recipients as list."""
        config = WatcherConfig(email_recipients=["user1@example.com", "user2@example.com"])

        assert len(config.email_recipients) == 2
        assert "user1@example.com" in config.email_recipients

    @patch.dict(
        os.environ,
        {
            "AIRFLOW_WATCHER_SLACK_WEBHOOK_URL": "https://env-webhook.com",
            "AIRFLOW_WATCHER_SLACK_CHANNEL": "#env-channel",
        },
    )
    def test_load_from_env(self):
        """Test loading config from environment variables."""
        config = WatcherConfig()
        config._load_from_env()

        assert config.slack_webhook_url == "https://env-webhook.com"
        assert config.slack_channel == "#env-channel"

    @patch.dict(
        os.environ,
        {
            "AIRFLOW_WATCHER_SMTP_PORT": "465",
        },
    )
    def test_load_from_env_int_value(self):
        """Test loading integer config from environment."""
        config = WatcherConfig()
        config._load_from_env()

        assert config.smtp_port == 465

    @patch.dict(
        os.environ,
        {
            "AIRFLOW_WATCHER_EMAIL_RECIPIENTS": "user1@test.com, user2@test.com",
        },
    )
    def test_load_from_env_list_value(self):
        """Test loading list config from environment."""
        config = WatcherConfig()
        config._load_from_env()

        assert len(config.email_recipients) == 2
        assert "user1@test.com" in config.email_recipients
        assert "user2@test.com" in config.email_recipients
