"""Standalone configuration for Airflow Watcher API.

Reads all settings from AIRFLOW_WATCHER_* environment variables.
No Airflow config dependency — suitable for running outside the Airflow webserver.
"""

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class StandaloneConfig:
    """Configuration for the standalone Airflow Watcher API service.

    All values are loaded from AIRFLOW_WATCHER_* environment variables.
    """

    # --- API-specific fields ---
    db_uri: str = ""
    api_host: str = "0.0.0.0"
    api_port: int = 8081
    api_keys: List[str] = field(default_factory=list)
    rbac_enabled: bool = False
    rbac_key_dag_mapping: Dict[str, List[str]] = field(default_factory=dict)
    cache_ttl: int = 60
    rate_limit_rpm: int = 120
    query_timeout_ms: int = 30000
    rbac_fail_open: bool = True

    # --- Slack settings ---
    slack_webhook_url: Optional[str] = None
    slack_token: Optional[str] = None
    slack_channel: str = "#airflow-alerts"

    # --- Email settings ---
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_user: Optional[str] = None
    smtp_password: Optional[str] = None
    email_from: str = "airflow-watcher@example.com"
    email_recipients: List[str] = field(default_factory=list)

    # --- PagerDuty settings ---
    pagerduty_routing_key: Optional[str] = None
    pagerduty_service_name: str = "Airflow Watcher"

    # --- StatsD settings ---
    statsd_enabled: bool = False
    statsd_host: str = "localhost"
    statsd_port: int = 8125
    statsd_prefix: str = "airflow.watcher"
    use_dogstatsd: bool = False

    # --- Prometheus settings ---
    prometheus_enabled: bool = False
    prometheus_prefix: str = "airflow_watcher"

    # --- General settings ---
    airflow_base_url: str = "http://localhost:8080"
    alert_rules_file: Optional[str] = None
    alert_template: str = "production_balanced"

    @classmethod
    def from_env(cls) -> "StandaloneConfig":
        """Load configuration from AIRFLOW_WATCHER_* environment variables.

        Returns:
            StandaloneConfig instance populated from the environment.

        Raises:
            SystemExit: If required configuration values are missing.
        """
        config = cls()
        config._load_from_env()
        config._validate()
        return config

    def _load_from_env(self) -> None:
        """Load configuration from environment variables.

        Replicates the env-var mapping logic from WatcherConfig._load_from_env()
        and adds the standalone API-specific variables.
        """
        # API-specific env vars
        api_env_mappings: Dict[str, str] = {
            "AIRFLOW_WATCHER_DB_URI": "db_uri",
            "AIRFLOW_WATCHER_API_HOST": "api_host",
            "AIRFLOW_WATCHER_API_PORT": "api_port",
            "AIRFLOW_WATCHER_API_KEYS": "api_keys",
            "AIRFLOW_WATCHER_RBAC_ENABLED": "rbac_enabled",
            "AIRFLOW_WATCHER_RBAC_KEY_DAG_MAPPING": "rbac_key_dag_mapping",
            "AIRFLOW_WATCHER_CACHE_TTL": "cache_ttl",
            "AIRFLOW_WATCHER_RATE_LIMIT_RPM": "rate_limit_rpm",
            "AIRFLOW_WATCHER_QUERY_TIMEOUT_MS": "query_timeout_ms",
            "AIRFLOW_WATCHER_RBAC_FAIL_OPEN": "rbac_fail_open",
        }

        # Notification / metrics env vars (same as WatcherConfig)
        shared_env_mappings: Dict[str, str] = {
            # Slack
            "AIRFLOW_WATCHER_SLACK_WEBHOOK_URL": "slack_webhook_url",
            "AIRFLOW_WATCHER_SLACK_TOKEN": "slack_token",
            "AIRFLOW_WATCHER_SLACK_CHANNEL": "slack_channel",
            # Email
            "AIRFLOW_WATCHER_SMTP_HOST": "smtp_host",
            "AIRFLOW_WATCHER_SMTP_PORT": "smtp_port",
            "AIRFLOW_WATCHER_SMTP_USER": "smtp_user",
            "AIRFLOW_WATCHER_SMTP_PASSWORD": "smtp_password",
            "AIRFLOW_WATCHER_EMAIL_FROM": "email_from",
            "AIRFLOW_WATCHER_EMAIL_RECIPIENTS": "email_recipients",
            # PagerDuty
            "AIRFLOW_WATCHER_PAGERDUTY_ROUTING_KEY": "pagerduty_routing_key",
            "AIRFLOW_WATCHER_PAGERDUTY_SERVICE_NAME": "pagerduty_service_name",
            # StatsD
            "AIRFLOW_WATCHER_STATSD_ENABLED": "statsd_enabled",
            "AIRFLOW_WATCHER_STATSD_HOST": "statsd_host",
            "AIRFLOW_WATCHER_STATSD_PORT": "statsd_port",
            "AIRFLOW_WATCHER_STATSD_PREFIX": "statsd_prefix",
            "AIRFLOW_WATCHER_USE_DOGSTATSD": "use_dogstatsd",
            # Prometheus
            "AIRFLOW_WATCHER_PROMETHEUS_ENABLED": "prometheus_enabled",
            "AIRFLOW_WATCHER_PROMETHEUS_PREFIX": "prometheus_prefix",
            # General
            "AIRFLOW_WATCHER_BASE_URL": "airflow_base_url",
            "AIRFLOW_WATCHER_ALERT_RULES_FILE": "alert_rules_file",
            "AIRFLOW_WATCHER_ALERT_TEMPLATE": "alert_template",
        }

        all_mappings = {**api_env_mappings, **shared_env_mappings}

        _INT_FIELDS = {"smtp_port", "statsd_port", "api_port", "cache_ttl", "rate_limit_rpm", "query_timeout_ms"}
        _BOOL_FIELDS = {"statsd_enabled", "use_dogstatsd", "prometheus_enabled", "rbac_enabled", "rbac_fail_open"}
        _CSV_FIELDS = {"email_recipients", "api_keys"}
        _JSON_FIELDS = {"rbac_key_dag_mapping"}

        for env_var, attr in all_mappings.items():
            value = os.environ.get(env_var)
            if value is None:
                continue

            if attr in _INT_FIELDS:
                try:
                    int_val = int(value)
                except ValueError:
                    logger.error("Invalid integer value for %s: %s", env_var, value)
                    raise ValueError(f"Invalid integer value for {env_var}: {value}")
                # Port range validation
                if attr in ("smtp_port", "statsd_port", "api_port") and not (1 <= int_val <= 65535):
                    raise ValueError(f"Port {value} out of range (1-65535) for {env_var}")
                setattr(self, attr, int_val)
            elif attr in _BOOL_FIELDS:
                setattr(self, attr, value.lower() in ("true", "1", "yes"))
            elif attr in _CSV_FIELDS:
                setattr(self, attr, [item.strip() for item in value.split(",") if item.strip()])
            elif attr in _JSON_FIELDS:
                try:
                    setattr(self, attr, json.loads(value))
                except json.JSONDecodeError:
                    logger.error("Invalid JSON value for %s: %s", env_var, value)
                    raise ValueError(f"Invalid JSON value for {env_var}: {value}")
            else:
                setattr(self, attr, value)

    def _validate(self) -> None:
        """Validate that all required configuration values are present."""
        if not self.db_uri:
            logger.error(
                "Missing required environment variable: AIRFLOW_WATCHER_DB_URI. "
                "Set it to the Airflow metadata database connection string."
            )
            raise ValueError(
                "Missing required environment variable: AIRFLOW_WATCHER_DB_URI. "
                "Set it to the Airflow metadata database connection string."
            )
        # Warn if API_KEYS env var is explicitly set but resolved to nothing
        api_keys_raw = os.environ.get("AIRFLOW_WATCHER_API_KEYS")
        if api_keys_raw is not None and not self.api_keys:
            logger.warning(
                "AIRFLOW_WATCHER_API_KEYS is set but contains no valid keys. "
                "Authentication will be DISABLED. Remove the variable entirely "
                "to suppress this warning, or set valid keys."
            )
