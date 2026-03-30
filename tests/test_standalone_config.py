"""Tests for StandaloneConfig."""

import json

import pytest

from airflow_watcher.api.standalone_config import StandaloneConfig


class TestStandaloneConfigFromEnv:
    """Test StandaloneConfig.from_env() loading."""

    def test_loads_db_uri(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_WATCHER_DB_URI", "postgresql://user:pass@host/db")
        config = StandaloneConfig.from_env()
        assert config.db_uri == "postgresql://user:pass@host/db"

    def test_missing_db_uri_exits(self, monkeypatch):
        monkeypatch.delenv("AIRFLOW_WATCHER_DB_URI", raising=False)
        with pytest.raises(ValueError, match="AIRFLOW_WATCHER_DB_URI"):
            StandaloneConfig.from_env()

    def test_default_host_and_port(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_WATCHER_DB_URI", "sqlite:///test.db")
        monkeypatch.delenv("AIRFLOW_WATCHER_API_PORT", raising=False)
        monkeypatch.delenv("AIRFLOW_WATCHER_API_HOST", raising=False)
        monkeypatch.delenv("AIRFLOW_WATCHER_API_KEYS", raising=False)
        config = StandaloneConfig.from_env()
        assert config.api_host == "0.0.0.0"
        assert config.api_port == 8081

    def test_custom_host_and_port(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_WATCHER_DB_URI", "sqlite:///test.db")
        monkeypatch.setenv("AIRFLOW_WATCHER_API_HOST", "127.0.0.1")
        monkeypatch.setenv("AIRFLOW_WATCHER_API_PORT", "9090")
        config = StandaloneConfig.from_env()
        assert config.api_host == "127.0.0.1"
        assert config.api_port == 9090

    def test_api_keys_csv(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_WATCHER_DB_URI", "sqlite:///test.db")
        monkeypatch.setenv("AIRFLOW_WATCHER_API_KEYS", "key1, key2, key3")
        config = StandaloneConfig.from_env()
        assert config.api_keys == ["key1", "key2", "key3"]

    def test_api_keys_empty_default(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_WATCHER_DB_URI", "sqlite:///test.db")
        monkeypatch.delenv("AIRFLOW_WATCHER_API_KEYS", raising=False)
        config = StandaloneConfig.from_env()
        assert config.api_keys == []

    def test_rbac_enabled_bool(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_WATCHER_DB_URI", "sqlite:///test.db")
        monkeypatch.setenv("AIRFLOW_WATCHER_RBAC_ENABLED", "true")
        config = StandaloneConfig.from_env()
        assert config.rbac_enabled is True

    def test_rbac_disabled_by_default(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_WATCHER_DB_URI", "sqlite:///test.db")
        config = StandaloneConfig.from_env()
        assert config.rbac_enabled is False

    def test_rbac_key_dag_mapping_json(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_WATCHER_DB_URI", "sqlite:///test.db")
        mapping = {"key1": ["dag_a", "dag_b"], "key2": ["dag_c"]}
        monkeypatch.setenv("AIRFLOW_WATCHER_RBAC_KEY_DAG_MAPPING", json.dumps(mapping))
        config = StandaloneConfig.from_env()
        assert config.rbac_key_dag_mapping == mapping

    def test_cache_ttl(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_WATCHER_DB_URI", "sqlite:///test.db")
        monkeypatch.setenv("AIRFLOW_WATCHER_CACHE_TTL", "120")
        config = StandaloneConfig.from_env()
        assert config.cache_ttl == 120

    def test_slack_settings(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_WATCHER_DB_URI", "sqlite:///test.db")
        monkeypatch.setenv("AIRFLOW_WATCHER_SLACK_WEBHOOK_URL", "https://hooks.slack.com/test")
        monkeypatch.setenv("AIRFLOW_WATCHER_SLACK_CHANNEL", "#ops")
        config = StandaloneConfig.from_env()
        assert config.slack_webhook_url == "https://hooks.slack.com/test"
        assert config.slack_channel == "#ops"

    def test_email_recipients_csv(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_WATCHER_DB_URI", "sqlite:///test.db")
        monkeypatch.setenv("AIRFLOW_WATCHER_EMAIL_RECIPIENTS", "a@b.com, c@d.com")
        config = StandaloneConfig.from_env()
        assert config.email_recipients == ["a@b.com", "c@d.com"]

    def test_statsd_settings(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_WATCHER_DB_URI", "sqlite:///test.db")
        monkeypatch.setenv("AIRFLOW_WATCHER_STATSD_ENABLED", "yes")
        monkeypatch.setenv("AIRFLOW_WATCHER_STATSD_HOST", "statsd.local")
        monkeypatch.setenv("AIRFLOW_WATCHER_STATSD_PORT", "9125")
        monkeypatch.setenv("AIRFLOW_WATCHER_STATSD_PREFIX", "myapp.watcher")
        config = StandaloneConfig.from_env()
        assert config.statsd_enabled is True
        assert config.statsd_host == "statsd.local"
        assert config.statsd_port == 9125
        assert config.statsd_prefix == "myapp.watcher"

    def test_prometheus_settings(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_WATCHER_DB_URI", "sqlite:///test.db")
        monkeypatch.setenv("AIRFLOW_WATCHER_PROMETHEUS_ENABLED", "1")
        monkeypatch.setenv("AIRFLOW_WATCHER_PROMETHEUS_PREFIX", "custom_prefix")
        config = StandaloneConfig.from_env()
        assert config.prometheus_enabled is True
        assert config.prometheus_prefix == "custom_prefix"

    def test_invalid_port_exits(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_WATCHER_DB_URI", "sqlite:///test.db")
        monkeypatch.setenv("AIRFLOW_WATCHER_API_PORT", "not_a_number")
        with pytest.raises(ValueError, match="Invalid integer value"):
            StandaloneConfig.from_env()

    def test_invalid_json_mapping_exits(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_WATCHER_DB_URI", "sqlite:///test.db")
        monkeypatch.setenv("AIRFLOW_WATCHER_RBAC_KEY_DAG_MAPPING", "not-json")
        with pytest.raises(ValueError, match="Invalid JSON value"):
            StandaloneConfig.from_env()

    def test_general_settings(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_WATCHER_DB_URI", "sqlite:///test.db")
        monkeypatch.setenv("AIRFLOW_WATCHER_BASE_URL", "https://airflow.example.com")
        monkeypatch.setenv("AIRFLOW_WATCHER_ALERT_TEMPLATE", "development")
        config = StandaloneConfig.from_env()
        assert config.airflow_base_url == "https://airflow.example.com"
        assert config.alert_template == "development"
