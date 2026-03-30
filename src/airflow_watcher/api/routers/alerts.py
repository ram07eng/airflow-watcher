"""Alert management endpoints."""

from typing import Optional

from fastapi import APIRouter, Depends

from airflow_watcher.api.auth import require_auth
from airflow_watcher.api.envelope import success_response
from airflow_watcher.alerting import AlertManager, AlertChannel
from airflow_watcher.alerting.rules import get_template_rules
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


@router.get("/rules")
def get_alert_rules(
    _auth: Optional[str] = Depends(require_auth),
):
    """List all configured alert rules."""
    manager = _get_manager()
    rules = manager.get_rules()

    return success_response([
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
    ])


@router.post("/evaluate")
def evaluate_alerts(
    _auth: Optional[str] = Depends(require_auth),
):
    """Evaluate alert rules against current metrics and dispatch notifications."""
    manager = _get_manager()
    collector = MetricsCollector()
    metrics = collector.collect()

    alerts = manager.evaluate_metrics(metrics)

    results = []
    for alert in alerts:
        send_result = manager.send_alert(alert)
        results.append({
            "rule_name": alert.rule_name,
            "metric": alert.metric,
            "current_value": alert.current_value,
            "threshold": alert.threshold,
            "severity": alert.severity.value,
            "message": alert.message,
            "channels": {ch.value: ok for ch, ok in send_result.items()},
        })

    return success_response({
        "evaluated_rules": len(manager.get_rules()),
        "triggered_alerts": len(results),
        "results": results,
    })
