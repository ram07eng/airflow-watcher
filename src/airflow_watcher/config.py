"""Configuration for Airflow Watcher."""

import os
from dataclasses import dataclass, field
from typing import List, Optional

from airflow.configuration import conf


@dataclass
class WatcherConfig:
    """Configuration settings for Airflow Watcher plugin."""

    # Slack settings
    slack_webhook_url: Optional[str] = None
    slack_token: Optional[str] = None
    slack_channel: str = "#airflow-alerts"

    # Email settings
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_use_tls: bool = True
    smtp_user: Optional[str] = None
    smtp_password: Optional[str] = None
    email_from: str = "airflow-watcher@example.com"
    email_recipients: List[str] = field(default_factory=list)

    # Monitoring settings
    failure_lookback_hours: int = 24
    sla_check_interval_minutes: int = 5
    sla_warning_threshold_minutes: int = 30

    # Alert settings
    alert_on_first_failure: bool = True
    alert_on_retry_failure: bool = False
    batch_alerts: bool = False
    batch_interval_minutes: int = 15

    @classmethod
    def from_airflow_config(cls) -> "WatcherConfig":
        """Load configuration from Airflow config.
        
        Returns:
            WatcherConfig instance with values from airflow.cfg
        """
        config = cls()
        
        try:
            # Slack settings
            config.slack_webhook_url = conf.get("airflow_watcher", "slack_webhook_url", fallback=None)
            config.slack_token = conf.get("airflow_watcher", "slack_token", fallback=None)
            config.slack_channel = conf.get("airflow_watcher", "slack_channel", fallback="#airflow-alerts")
            
            # Email settings
            config.smtp_host = conf.get("airflow_watcher", "smtp_host", fallback=None)
            config.smtp_port = conf.getint("airflow_watcher", "smtp_port", fallback=587)
            config.smtp_use_tls = conf.getboolean("airflow_watcher", "smtp_use_tls", fallback=True)
            config.smtp_user = conf.get("airflow_watcher", "smtp_user", fallback=None)
            config.smtp_password = conf.get("airflow_watcher", "smtp_password", fallback=None)
            config.email_from = conf.get("airflow_watcher", "email_from", fallback="airflow-watcher@example.com")
            
            recipients_str = conf.get("airflow_watcher", "email_recipients", fallback="")
            config.email_recipients = [r.strip() for r in recipients_str.split(",") if r.strip()]
            
            # Monitoring settings
            config.failure_lookback_hours = conf.getint("airflow_watcher", "failure_lookback_hours", fallback=24)
            config.sla_check_interval_minutes = conf.getint("airflow_watcher", "sla_check_interval_minutes", fallback=5)
            config.sla_warning_threshold_minutes = conf.getint("airflow_watcher", "sla_warning_threshold_minutes", fallback=30)
            
            # Alert settings
            config.alert_on_first_failure = conf.getboolean("airflow_watcher", "alert_on_first_failure", fallback=True)
            config.alert_on_retry_failure = conf.getboolean("airflow_watcher", "alert_on_retry_failure", fallback=False)
            config.batch_alerts = conf.getboolean("airflow_watcher", "batch_alerts", fallback=False)
            config.batch_interval_minutes = conf.getint("airflow_watcher", "batch_interval_minutes", fallback=15)
            
        except Exception:
            # If airflow_watcher section doesn't exist, use defaults
            pass
        
        # Override with environment variables
        config._load_from_env()
        
        return config

    def _load_from_env(self) -> None:
        """Load configuration from environment variables."""
        env_mappings = {
            "AIRFLOW_WATCHER_SLACK_WEBHOOK_URL": "slack_webhook_url",
            "AIRFLOW_WATCHER_SLACK_TOKEN": "slack_token",
            "AIRFLOW_WATCHER_SLACK_CHANNEL": "slack_channel",
            "AIRFLOW_WATCHER_SMTP_HOST": "smtp_host",
            "AIRFLOW_WATCHER_SMTP_PORT": "smtp_port",
            "AIRFLOW_WATCHER_SMTP_USER": "smtp_user",
            "AIRFLOW_WATCHER_SMTP_PASSWORD": "smtp_password",
            "AIRFLOW_WATCHER_EMAIL_FROM": "email_from",
            "AIRFLOW_WATCHER_EMAIL_RECIPIENTS": "email_recipients",
        }
        
        for env_var, attr in env_mappings.items():
            value = os.environ.get(env_var)
            if value is not None:
                if attr == "smtp_port":
                    setattr(self, attr, int(value))
                elif attr == "email_recipients":
                    setattr(self, attr, [r.strip() for r in value.split(",") if r.strip()])
                else:
                    setattr(self, attr, value)
