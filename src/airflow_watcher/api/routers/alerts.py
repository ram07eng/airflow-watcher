"""Alert management endpoints."""

from typing import Optional

from fastapi import APIRouter, Depends

from airflow_watcher.alerting import AlertManager
from airflow_watcher.api.auth import require_auth
from airflow_watcher.api.envelope import success_response
from airflow_watcher.metrics.collector import MetricsCollector

router = APIRouter(prefix="/alerts", tags=["alerts"])

# Module-level reference configured at startup.
_alert_manager: Optional[AlertManager] = None


def configure_alerts(manager: AlertManager) -> None:
    """Set the alert manager.  Called once at startup."""
    global _alert_manager
    _alert_manager = manager


def _get_manager() -> AlertManager:
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
    return _alert_manager


@router.get(
    "/rules",
    summary="List configured alert rules",
    response_description="All alert rules with thresholds, severity, and notification channels",
)
def get_alert_rules(
    _auth: Optional[str] = Depends(require_auth),
):
    """Return all configured alert rules.

    Each rule includes `metric`, `condition`, `threshold`, `severity`,
    `channels` (Slack / email / PagerDuty), `cooldown_minutes`, and `enabled` flag.
    """
    manager = _get_manager()
    rules = manager.get_rules()

    return success_response(
        [
            {
                "name": r.name,
                "metric": r.metric,
                "condition": r.condition,
                "threshold": r.threshold,
                "severity": r.severity.value,
                "channels": [c.value for c in r.channels],
                "cooldown_minutes": r.cooldown_minutes,
                "enabled": r.enabled,
            }
            for r in rules
        ]
    )


@router.post(
    "/evaluate",
    summary="Evaluate rules and dispatch alerts",
    response_description="Triggered alerts with current values and dispatch results per channel",
)
def evaluate_alerts(
    _auth: Optional[str] = Depends(require_auth),
):
    """Evaluate all alert rules against live metrics and fire notifications.

    For each rule that breaches its threshold, dispatches to configured
    channels (Slack webhook, email, PagerDuty) and returns per-channel
    send status.

    Returns `evaluated_rules` count and a `results` list of triggered alerts.
    Rules on cooldown are skipped.
    """
    manager = _get_manager()
    collector = MetricsCollector()
    metrics = collector.collect()

    alerts = manager.evaluate_metrics(metrics)

    results = []
    for alert in alerts:
        send_result = manager.send_alert(alert)
        results.append(
            {
                "rule_name": alert.rule_name,
                "metric": alert.metric,
                "current_value": alert.current_value,
                "threshold": alert.threshold,
                "severity": alert.severity.value,
                "message": alert.message,
                "channels": {ch.value: ok for ch, ok in send_result.items()},
            }
        )

    return success_response(
        {
            "evaluated_rules": len(manager.get_rules()),
            "triggered_alerts": len(results),
            "results": results,
        }
    )
