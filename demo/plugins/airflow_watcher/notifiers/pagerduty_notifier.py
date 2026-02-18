"""PagerDuty Notifier for Airflow Watcher alerts."""

import logging
import hashlib
from typing import Optional, Dict, Any, List

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

from airflow.utils import timezone

from airflow_watcher.models.failure import DAGFailure
from airflow_watcher.models.sla import SLAMissEvent
from airflow_watcher.config import WatcherConfig


logger = logging.getLogger(__name__)


class PagerDutyNotifier:
    """Sends alerts to PagerDuty."""

    # PagerDuty Events API v2 endpoint
    EVENTS_API_URL = "https://events.pagerduty.com/v2/enqueue"

    def __init__(self, config: Optional[WatcherConfig] = None):
        """Initialize PagerDuty notifier.

        Args:
            config: Configuration object with PagerDuty settings
        """
        if not HAS_REQUESTS:
            raise ImportError("requests library required for PagerDuty integration")

        self.config = config or WatcherConfig()
        self.routing_key = self.config.pagerduty_routing_key
        self.service_name = self.config.pagerduty_service_name or "Airflow Watcher"

    def _generate_dedup_key(self, source: str, identifier: str) -> str:
        """Generate a deduplication key for PagerDuty."""
        key_string = f"{source}:{identifier}"
        return hashlib.md5(key_string.encode()).hexdigest()

    def _send_event(
        self,
        summary: str,
        severity: str,
        source: str,
        dedup_key: str,
        custom_details: Optional[Dict[str, Any]] = None,
        event_action: str = "trigger",
    ) -> bool:
        """Send an event to PagerDuty.

        Args:
            summary: Event summary (max 1024 chars)
            severity: critical, error, warning, or info
            source: Source of the alert
            dedup_key: Deduplication key
            custom_details: Additional details
            event_action: trigger, acknowledge, or resolve

        Returns:
            True if event was sent successfully
        """
        if not self.routing_key:
            logger.warning("PagerDuty routing key not configured")
            return False

        payload = {
            "routing_key": self.routing_key,
            "event_action": event_action,
            "dedup_key": dedup_key,
            "payload": {
                "summary": summary[:1024],
                "severity": severity,
                "source": source,
                "timestamp": timezone.utcnow().isoformat(),
                "custom_details": custom_details or {},
            },
        }

        try:
            response = requests.post(
                self.EVENTS_API_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )

            if response.status_code == 202:
                logger.info(f"PagerDuty event sent: {dedup_key}")
                return True
            else:
                logger.error(f"PagerDuty API error: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Failed to send PagerDuty event: {e}")
            return False

    def send_failure_alert(self, failure: DAGFailure, severity: str = "error") -> bool:
        """Send a DAG failure alert to PagerDuty.

        Args:
            failure: DAGFailure object with failure details
            severity: Alert severity (critical, error, warning, info)

        Returns:
            True if alert was sent successfully
        """
        failed_task_ids = [t.task_id for t in failure.failed_tasks]
        summary = f"DAG Failed: {failure.dag_id}"
        if failed_task_ids:
            summary += f" (tasks: {', '.join(failed_task_ids[:3])})"

        dedup_key = self._generate_dedup_key("dag_failure", f"{failure.dag_id}:{failure.run_id}")

        custom_details = {
            "dag_id": failure.dag_id,
            "run_id": failure.run_id,
            "execution_date": str(failure.execution_date),
            "start_date": str(failure.start_date) if failure.start_date else None,
            "end_date": str(failure.end_date) if failure.end_date else None,
            "failed_task_count": len(failure.failed_tasks),
            "failed_tasks": failed_task_ids[:10],
            "external_trigger": failure.external_trigger,
            "airflow_url": self.config.airflow_base_url,
        }

        return self._send_event(
            summary=summary,
            severity=severity,
            source=self.service_name,
            dedup_key=dedup_key,
            custom_details=custom_details,
        )

    def send_sla_miss_alert(self, sla_miss: SLAMissEvent, severity: str = "warning") -> bool:
        """Send an SLA miss alert to PagerDuty.

        Args:
            sla_miss: SLAMissEvent object with SLA miss details
            severity: Alert severity

        Returns:
            True if alert was sent successfully
        """
        summary = f"SLA Missed: {sla_miss.dag_id}/{sla_miss.task_id}"

        dedup_key = self._generate_dedup_key(
            "sla_miss",
            f"{sla_miss.dag_id}:{sla_miss.task_id}:{sla_miss.execution_date}",
        )

        custom_details = {
            "dag_id": sla_miss.dag_id,
            "task_id": sla_miss.task_id,
            "execution_date": str(sla_miss.execution_date),
            "timestamp": str(sla_miss.timestamp),
            "description": sla_miss.description,
            "email_sent": sla_miss.email_sent,
            "notification_sent": sla_miss.notification_sent,
            "airflow_url": self.config.airflow_base_url,
        }

        return self._send_event(
            summary=summary,
            severity=severity,
            source=self.service_name,
            dedup_key=dedup_key,
            custom_details=custom_details,
        )

    def send_batch_alert(self, failures: List[DAGFailure], severity: str = "error") -> bool:
        """Send a batch failure alert to PagerDuty.

        Args:
            failures: List of DAGFailure objects
            severity: Alert severity

        Returns:
            True if alert was sent successfully
        """
        if not failures:
            return True

        dag_ids = list(set(f.dag_id for f in failures))
        summary = f"{len(failures)} DAG failures: {', '.join(dag_ids[:5])}"
        if len(dag_ids) > 5:
            summary += f" and {len(dag_ids) - 5} more"

        dedup_key = self._generate_dedup_key(
            "batch_failure",
            timezone.utcnow().strftime("%Y-%m-%d-%H"),
        )

        custom_details = {
            "total_failures": len(failures),
            "unique_dags": len(dag_ids),
            "dag_ids": dag_ids[:20],
            "first_failure": str(min(f.execution_date for f in failures)),
            "last_failure": str(max(f.execution_date for f in failures)),
            "airflow_url": self.config.airflow_base_url,
        }

        return self._send_event(
            summary=summary,
            severity=severity,
            source=self.service_name,
            dedup_key=dedup_key,
            custom_details=custom_details,
        )

    def send_threshold_alert(
        self,
        metric_name: str,
        current_value: float,
        threshold: float,
        severity: str = "warning",
    ) -> bool:
        """Send a threshold breach alert.

        Args:
            metric_name: Name of the metric
            current_value: Current metric value
            threshold: Threshold that was breached
            severity: Alert severity

        Returns:
            True if alert was sent successfully
        """
        summary = f"Threshold Breached: {metric_name} = {current_value} (threshold: {threshold})"
        dedup_key = self._generate_dedup_key("threshold", metric_name)

        custom_details = {
            "metric_name": metric_name,
            "current_value": current_value,
            "threshold": threshold,
            "breach_type": "above" if current_value > threshold else "below",
            "airflow_url": self.config.airflow_base_url,
        }

        return self._send_event(
            summary=summary,
            severity=severity,
            source=self.service_name,
            dedup_key=dedup_key,
            custom_details=custom_details,
        )

    def resolve_alert(self, dedup_key: str) -> bool:
        """Resolve a previously triggered alert.

        Args:
            dedup_key: Deduplication key of the alert to resolve

        Returns:
            True if resolution was sent successfully
        """
        if not self.routing_key:
            return False

        payload = {
            "routing_key": self.routing_key,
            "event_action": "resolve",
            "dedup_key": dedup_key,
        }

        try:
            response = requests.post(
                self.EVENTS_API_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            return response.status_code == 202
        except Exception as e:
            logger.error(f"Failed to resolve PagerDuty alert: {e}")
            return False

    def send_test_alert(self) -> bool:
        """Send a test event to verify PagerDuty configuration.

        Returns:
            True if test event was sent successfully
        """
        return self._send_event(
            summary="Airflow Watcher test alert - configuration verified",
            severity="info",
            source=self.service_name,
            dedup_key=self._generate_dedup_key("test", "airflow-watcher"),
        )

