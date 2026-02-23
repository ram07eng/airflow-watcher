"""Alert Rules Configuration - Easy setup for alerting."""

import json
import logging
import os
from typing import Dict, List, Optional

from airflow_watcher.alerting import AlertChannel, AlertRule, AlertSeverity

logger = logging.getLogger(__name__)


# Pre-defined rule templates for common scenarios
RULE_TEMPLATES = {
    "production_strict": [
        AlertRule(
            name="any_failure",
            metric="failures.total_24h",
            condition="gte",
            threshold=1,
            severity=AlertSeverity.ERROR,
            channels=[AlertChannel.SLACK, AlertChannel.PAGERDUTY],
            cooldown_minutes=5,
        ),
        AlertRule(
            name="sla_miss",
            metric="sla.misses_24h",
            condition="gte",
            threshold=1,
            severity=AlertSeverity.WARNING,
            channels=[AlertChannel.SLACK],
            cooldown_minutes=5,
        ),
        AlertRule(
            name="dag_health_critical",
            metric="dags.healthy_percent",
            condition="lt",
            threshold=90,
            severity=AlertSeverity.CRITICAL,
            channels=[AlertChannel.SLACK, AlertChannel.PAGERDUTY, AlertChannel.EMAIL],
            cooldown_minutes=5,
        ),
    ],
    "production_balanced": [
        AlertRule(
            name="multiple_failures",
            metric="failures.total_24h",
            condition="gte",
            threshold=5,
            severity=AlertSeverity.WARNING,
            channels=[AlertChannel.SLACK],
            cooldown_minutes=15,
        ),
        AlertRule(
            name="high_failure_count",
            metric="failures.total_24h",
            condition="gte",
            threshold=10,
            severity=AlertSeverity.ERROR,
            channels=[AlertChannel.SLACK, AlertChannel.PAGERDUTY],
            cooldown_minutes=15,
        ),
        AlertRule(
            name="sla_misses",
            metric="sla.misses_24h",
            condition="gte",
            threshold=3,
            severity=AlertSeverity.WARNING,
            channels=[AlertChannel.SLACK],
            cooldown_minutes=30,
        ),
        AlertRule(
            name="dag_health_degraded",
            metric="dags.healthy_percent",
            condition="lt",
            threshold=80,
            severity=AlertSeverity.ERROR,
            channels=[AlertChannel.SLACK, AlertChannel.PAGERDUTY],
            cooldown_minutes=15,
        ),
    ],
    "production_relaxed": [
        AlertRule(
            name="many_failures",
            metric="failures.total_24h",
            condition="gte",
            threshold=20,
            severity=AlertSeverity.ERROR,
            channels=[AlertChannel.SLACK],
            cooldown_minutes=60,
        ),
        AlertRule(
            name="critical_failure_count",
            metric="failures.total_24h",
            condition="gte",
            threshold=50,
            severity=AlertSeverity.CRITICAL,
            channels=[AlertChannel.SLACK, AlertChannel.PAGERDUTY],
            cooldown_minutes=30,
        ),
        AlertRule(
            name="dag_health_poor",
            metric="dags.healthy_percent",
            condition="lt",
            threshold=60,
            severity=AlertSeverity.CRITICAL,
            channels=[AlertChannel.SLACK, AlertChannel.PAGERDUTY],
            cooldown_minutes=30,
        ),
    ],
    "development": [
        AlertRule(
            name="high_failure_count",
            metric="failures.total_24h",
            condition="gte",
            threshold=50,
            severity=AlertSeverity.INFO,
            channels=[AlertChannel.SLACK],
            cooldown_minutes=120,
        ),
    ],
}


def get_template_rules(template_name: str) -> List[AlertRule]:
    """Get alert rules from a predefined template.

    Args:
        template_name: One of: production_strict, production_balanced,
                       production_relaxed, development

    Returns:
        List of AlertRule objects
    """
    return RULE_TEMPLATES.get(template_name, [])


def list_templates() -> List[str]:
    """List available rule templates."""
    return list(RULE_TEMPLATES.keys())


def create_custom_rule(
    name: str,
    metric: str,
    condition: str,
    threshold: float,
    severity: str = "warning",
    channels: Optional[List[str]] = None,
    cooldown_minutes: int = 15,
    dag_filter: Optional[str] = None,
    owner_filter: Optional[str] = None,
    tag_filter: Optional[str] = None,
) -> AlertRule:
    """Create a custom alert rule.

    Args:
        name: Unique rule name
        metric: Metric to evaluate (e.g., "failures.total_24h")
        condition: Comparison operator ("gt", "lt", "eq", "gte", "lte")
        threshold: Threshold value
        severity: Alert severity ("info", "warning", "error", "critical")
        channels: List of channels ("slack", "email", "pagerduty")
        cooldown_minutes: Minutes between repeated alerts
        dag_filter: Regex pattern to filter DAG IDs
        owner_filter: Filter by DAG owner
        tag_filter: Filter by DAG tag

    Returns:
        AlertRule object
    """
    severity_map = {
        "info": AlertSeverity.INFO,
        "warning": AlertSeverity.WARNING,
        "error": AlertSeverity.ERROR,
        "critical": AlertSeverity.CRITICAL,
    }

    channel_map = {
        "slack": AlertChannel.SLACK,
        "email": AlertChannel.EMAIL,
        "pagerduty": AlertChannel.PAGERDUTY,
    }

    channels = channels or ["slack"]
    alert_channels = [channel_map[c.lower()] for c in channels if c.lower() in channel_map]

    return AlertRule(
        name=name,
        metric=metric,
        condition=condition,
        threshold=threshold,
        severity=severity_map.get(severity.lower(), AlertSeverity.WARNING),
        channels=alert_channels,
        cooldown_minutes=cooldown_minutes,
        dag_filter=dag_filter,
        owner_filter=owner_filter,
        tag_filter=tag_filter,
    )


def load_rules_from_file(filepath: str) -> List[AlertRule]:
    """Load alert rules from a JSON file.

    Args:
        filepath: Path to JSON file

    Returns:
        List of AlertRule objects
    """
    if not os.path.exists(filepath):
        logger.warning(f"Rules file not found: {filepath}")
        return []

    try:
        with open(filepath, "r") as f:
            rules_data = json.load(f)

        rules = []
        for rule_dict in rules_data:
            rule = create_custom_rule(**rule_dict)
            rules.append(rule)

        logger.info(f"Loaded {len(rules)} alert rules from {filepath}")
        return rules

    except Exception as e:
        logger.error(f"Failed to load rules from {filepath}: {e}")
        return []


def save_rules_to_file(rules: List[AlertRule], filepath: str):
    """Save alert rules to a JSON file.

    Args:
        rules: List of AlertRule objects
        filepath: Path to save JSON file
    """
    rules_data = []
    for rule in rules:
        rules_data.append(
            {
                "name": rule.name,
                "metric": rule.metric,
                "condition": rule.condition,
                "threshold": rule.threshold,
                "severity": rule.severity.value,
                "channels": [c.value for c in rule.channels],
                "cooldown_minutes": rule.cooldown_minutes,
                "dag_filter": rule.dag_filter,
                "owner_filter": rule.owner_filter,
                "tag_filter": rule.tag_filter,
            }
        )

    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w") as f:
            json.dump(rules_data, f, indent=2)
        logger.info(f"Saved {len(rules)} alert rules to {filepath}")
    except Exception as e:
        logger.error(f"Failed to save rules to {filepath}: {e}")


def load_rules_from_env() -> List[AlertRule]:
    """Load alert rules from environment variables.

    Environment variable format:
        WATCHER_ALERT_RULE_1='{"name":"rule1","metric":"failures.total_24h",...}'
        WATCHER_ALERT_RULE_2='{"name":"rule2",...}'

    Returns:
        List of AlertRule objects
    """
    rules = []
    i = 1

    while True:
        env_var = f"WATCHER_ALERT_RULE_{i}"
        rule_json = os.environ.get(env_var)

        if not rule_json:
            break

        try:
            rule_dict = json.loads(rule_json)
            rule = create_custom_rule(**rule_dict)
            rules.append(rule)
        except Exception as e:
            logger.warning(f"Failed to parse {env_var}: {e}")

        i += 1

    if rules:
        logger.info(f"Loaded {len(rules)} alert rules from environment variables")

    return rules


# Available metrics for alerting
AVAILABLE_METRICS = {
    "failures.total_24h": "Total DAG failures in last 24 hours",
    "failures.unique_dags_24h": "Unique DAGs that failed in last 24 hours",
    "failures.rate_percent": "DAG failure rate as percentage",
    "sla.misses_24h": "SLA misses in last 24 hours",
    "sla.miss_rate_percent": "SLA miss rate as percentage",
    "tasks.failed_24h": "Failed tasks in last 24 hours",
    "tasks.retry_24h": "Tasks pending retry",
    "tasks.long_running": "Long-running tasks (>1 hour)",
    "scheduling.missed_24h": "Missed schedules in last 24 hours",
    "scheduling.delayed_dags": "DAGs with scheduling delays",
    "dags.unhealthy": "Number of unhealthy DAGs",
    "dags.total": "Total number of DAGs",
    "dags.healthy_percent": "Percentage of healthy DAGs",
    "dependencies.failed_sensors": "Failed sensor tasks",
    "dependencies.blocked_dags": "DAGs blocked by dependencies",
}


def list_available_metrics() -> Dict[str, str]:
    """Get list of available metrics for alerting."""
    return AVAILABLE_METRICS.copy()
