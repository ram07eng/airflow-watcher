"""Alert Manager - Coordinates alerting across multiple channels."""

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

from airflow.utils import timezone

from airflow_watcher.config import WatcherConfig
from airflow_watcher.metrics.collector import WatcherMetrics
from airflow_watcher.models.failure import DAGFailure
from airflow_watcher.models.sla import SLAMissEvent

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertChannel(Enum):
    """Available alert channels."""
    SLACK = "slack"
    EMAIL = "email"
    PAGERDUTY = "pagerduty"


@dataclass
class AlertRule:
    """Defines an alerting rule."""

    name: str
    metric: str
    condition: str  # "gt", "lt", "eq", "gte", "lte"
    threshold: float
    severity: AlertSeverity = AlertSeverity.WARNING
    channels: List[AlertChannel] = field(default_factory=lambda: [AlertChannel.SLACK])
    cooldown_minutes: int = 15
    enabled: bool = True

    # Optional filters
    dag_filter: Optional[str] = None  # Regex pattern for DAG IDs
    owner_filter: Optional[str] = None  # Filter by owner
    tag_filter: Optional[str] = None  # Filter by tag

    def evaluate(self, value: float) -> bool:
        """Evaluate if the rule condition is met.

        Args:
            value: Current metric value

        Returns:
            True if condition is met (should alert)
        """
        if not self.enabled:
            return False

        conditions = {
            "gt": value > self.threshold,
            "lt": value < self.threshold,
            "eq": value == self.threshold,
            "gte": value >= self.threshold,
            "lte": value <= self.threshold,
        }
        return conditions.get(self.condition, False)


@dataclass
class Alert:
    """Represents an alert to be sent."""

    rule_name: str
    metric: str
    current_value: float
    threshold: float
    severity: AlertSeverity
    message: str
    channels: List[AlertChannel]
    timestamp: datetime = field(default_factory=timezone.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


class AlertManager:
    """Manages alert routing and delivery to multiple channels."""

    # Default alert rules
    DEFAULT_RULES = [
        AlertRule(
            name="high_failure_count",
            metric="failures.total_24h",
            condition="gte",
            threshold=10,
            severity=AlertSeverity.ERROR,
            channels=[AlertChannel.SLACK, AlertChannel.PAGERDUTY],
        ),
        AlertRule(
            name="critical_failure_count",
            metric="failures.total_24h",
            condition="gte",
            threshold=25,
            severity=AlertSeverity.CRITICAL,
            channels=[AlertChannel.SLACK, AlertChannel.PAGERDUTY, AlertChannel.EMAIL],
        ),
        AlertRule(
            name="sla_misses",
            metric="sla.misses_24h",
            condition="gte",
            threshold=5,
            severity=AlertSeverity.WARNING,
            channels=[AlertChannel.SLACK],
        ),
        AlertRule(
            name="unhealthy_dags",
            metric="dags.unhealthy",
            condition="gte",
            threshold=5,
            severity=AlertSeverity.WARNING,
            channels=[AlertChannel.SLACK],
        ),
        AlertRule(
            name="low_dag_health",
            metric="dags.healthy_percent",
            condition="lt",
            threshold=80,
            severity=AlertSeverity.ERROR,
            channels=[AlertChannel.SLACK, AlertChannel.PAGERDUTY],
        ),
        AlertRule(
            name="long_running_tasks",
            metric="tasks.long_running",
            condition="gte",
            threshold=10,
            severity=AlertSeverity.WARNING,
            channels=[AlertChannel.SLACK],
        ),
    ]

    def __init__(self, config: Optional[WatcherConfig] = None):
        """Initialize alert manager.

        Args:
            config: Configuration object
        """
        self.config = config or WatcherConfig()
        self.rules: List[AlertRule] = list(self.DEFAULT_RULES)
        self._notifiers: Dict[AlertChannel, Any] = {}
        self._last_alerts: Dict[str, datetime] = {}  # rule_name -> last alert time
        self._lock = threading.Lock()

        self._init_notifiers()

    def _init_notifiers(self):
        """Initialize available notifiers based on config."""
        # Slack
        if self.config.slack_webhook_url or self.config.slack_token:
            try:
                from airflow_watcher.notifiers.slack_notifier import SlackNotifier
                self._notifiers[AlertChannel.SLACK] = SlackNotifier(self.config)
            except Exception as e:
                logger.warning(f"Failed to initialize Slack notifier: {e}")

        # Email
        if self.config.smtp_host:
            try:
                from airflow_watcher.notifiers.email_notifier import EmailNotifier
                self._notifiers[AlertChannel.EMAIL] = EmailNotifier(self.config)
            except Exception as e:
                logger.warning(f"Failed to initialize Email notifier: {e}")

        # PagerDuty
        if self.config.pagerduty_routing_key:
            try:
                from airflow_watcher.notifiers.pagerduty_notifier import PagerDutyNotifier
                self._notifiers[AlertChannel.PAGERDUTY] = PagerDutyNotifier(self.config)
            except Exception as e:
                logger.warning(f"Failed to initialize PagerDuty notifier: {e}")

    def add_rule(self, rule: AlertRule):
        """Add a custom alert rule.

        Args:
            rule: AlertRule to add
        """
        self.rules.append(rule)

    def remove_rule(self, rule_name: str):
        """Remove an alert rule by name.

        Args:
            rule_name: Name of the rule to remove
        """
        self.rules = [r for r in self.rules if r.name != rule_name]

    def get_rules(self) -> List[AlertRule]:
        """Get all configured rules."""
        return self.rules

    def update_rule(self, rule_name: str, **kwargs):
        """Update an existing rule.

        Args:
            rule_name: Name of the rule to update
            **kwargs: Fields to update
        """
        for rule in self.rules:
            if rule.name == rule_name:
                for key, value in kwargs.items():
                    if hasattr(rule, key):
                        setattr(rule, key, value)
                break

    def _should_alert(self, rule: AlertRule) -> bool:
        """Check if we should alert based on cooldown.

        Args:
            rule: The alert rule

        Returns:
            True if cooldown has passed
        """
        with self._lock:
            last_alert = self._last_alerts.get(rule.name)
            if last_alert is None:
                return True

            cooldown = timedelta(minutes=rule.cooldown_minutes)
            return timezone.utcnow() - last_alert >= cooldown

    def _record_alert(self, rule_name: str):
        """Record that an alert was sent."""
        with self._lock:
            self._last_alerts[rule_name] = timezone.utcnow()

    def evaluate_metrics(self, metrics: WatcherMetrics) -> List[Alert]:
        """Evaluate all rules against current metrics.

        Args:
            metrics: Current WatcherMetrics

        Returns:
            List of alerts to send
        """
        alerts = []
        metrics_dict = metrics.to_dict()

        for rule in self.rules:
            if not rule.enabled:
                continue

            if rule.metric not in metrics_dict:
                continue

            value = metrics_dict[rule.metric]

            if rule.evaluate(value) and self._should_alert(rule):
                alert = Alert(
                    rule_name=rule.name,
                    metric=rule.metric,
                    current_value=value,
                    threshold=rule.threshold,
                    severity=rule.severity,
                    message=f"{rule.name}: {rule.metric} = {value} (threshold: {rule.threshold})",
                    channels=rule.channels,
                )
                alerts.append(alert)

        return alerts

    def send_alert(self, alert: Alert) -> Dict[AlertChannel, bool]:
        """Send an alert to all configured channels.

        Args:
            alert: Alert to send

        Returns:
            Dict of channel -> success status
        """
        results = {}

        for channel in alert.channels:
            notifier = self._notifiers.get(channel)
            if not notifier:
                logger.debug(f"Notifier for {channel.value} not configured")
                results[channel] = False
                continue

            try:
                if channel == AlertChannel.SLACK:
                    success = notifier.send_threshold_alert(
                        metric_name=alert.metric,
                        current_value=alert.current_value,
                        threshold=alert.threshold,
                        severity=alert.severity.value,
                    )
                elif channel == AlertChannel.EMAIL:
                    success = notifier.send_threshold_alert(
                        metric_name=alert.metric,
                        current_value=alert.current_value,
                        threshold=alert.threshold,
                    )
                elif channel == AlertChannel.PAGERDUTY:
                    success = notifier.send_threshold_alert(
                        metric_name=alert.metric,
                        current_value=alert.current_value,
                        threshold=alert.threshold,
                        severity=alert.severity.value,
                    )
                else:
                    success = False

                results[channel] = success

            except Exception as e:
                logger.error(f"Failed to send alert via {channel.value}: {e}")
                results[channel] = False

        if any(results.values()):
            self._record_alert(alert.rule_name)

        return results

    def send_failure_alert(
        self,
        failure: DAGFailure,
        channels: Optional[List[AlertChannel]] = None,
    ) -> Dict[AlertChannel, bool]:
        """Send a DAG failure alert.

        Args:
            failure: DAGFailure object
            channels: Specific channels to use (default: all configured)

        Returns:
            Dict of channel -> success status
        """
        channels = channels or list(self._notifiers.keys())
        results = {}

        for channel in channels:
            notifier = self._notifiers.get(channel)
            if notifier and hasattr(notifier, "send_failure_alert"):
                try:
                    results[channel] = notifier.send_failure_alert(failure)
                except Exception as e:
                    logger.error(f"Failed to send failure alert via {channel.value}: {e}")
                    results[channel] = False

        return results

    def send_sla_alert(
        self,
        sla_miss: SLAMissEvent,
        channels: Optional[List[AlertChannel]] = None,
    ) -> Dict[AlertChannel, bool]:
        """Send an SLA miss alert.

        Args:
            sla_miss: SLAMissEvent object
            channels: Specific channels to use (default: all configured)

        Returns:
            Dict of channel -> success status
        """
        channels = channels or list(self._notifiers.keys())
        results = {}

        for channel in channels:
            notifier = self._notifiers.get(channel)
            if notifier and hasattr(notifier, "send_sla_miss_alert"):
                try:
                    results[channel] = notifier.send_sla_miss_alert(sla_miss)
                except Exception as e:
                    logger.error(f"Failed to send SLA alert via {channel.value}: {e}")
                    results[channel] = False

        return results

    def check_and_alert(self) -> List[Dict[str, Any]]:
        """Collect metrics, evaluate rules, and send alerts.

        Returns:
            List of alert results
        """
        from airflow_watcher.metrics.collector import MetricsCollector

        collector = MetricsCollector()
        metrics = collector.collect()

        alerts = self.evaluate_metrics(metrics)
        results = []

        for alert in alerts:
            send_results = self.send_alert(alert)
            results.append({
                "alert": alert,
                "channels": send_results,
            })

        return results

    def get_configured_channels(self) -> List[AlertChannel]:
        """Get list of configured alert channels."""
        return list(self._notifiers.keys())

    def test_channel(self, channel: AlertChannel) -> bool:
        """Send a test alert to a specific channel.

        Args:
            channel: Channel to test

        Returns:
            True if test was successful
        """
        notifier = self._notifiers.get(channel)
        if not notifier:
            return False

        try:
            if hasattr(notifier, "send_test_alert"):
                return notifier.send_test_alert()
            else:
                # Generic test
                if channel == AlertChannel.SLACK:
                    return notifier.webhook_client is not None or notifier.web_client is not None
                elif channel == AlertChannel.EMAIL:
                    return notifier.smtp_host is not None
                elif channel == AlertChannel.PAGERDUTY:
                    return notifier.routing_key is not None
        except Exception as e:
            logger.error(f"Failed to test channel {channel.value}: {e}")

        return False
