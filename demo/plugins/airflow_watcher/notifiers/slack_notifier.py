"""Slack Notifier for Airflow Watcher alerts."""

import logging
from typing import List, Optional

from slack_sdk import WebClient
from slack_sdk.webhook import WebhookClient
from slack_sdk.errors import SlackApiError

from airflow_watcher.models.failure import DAGFailure
from airflow_watcher.models.sla import SLAMissEvent
from airflow_watcher.config import WatcherConfig


logger = logging.getLogger(__name__)


class SlackNotifier:
    """Sends alerts to Slack channels."""

    def __init__(self, config: Optional[WatcherConfig] = None):
        """Initialize Slack notifier.
        
        Args:
            config: Configuration object with Slack settings
        """
        self.config = config or WatcherConfig()
        self.webhook_client = None
        self.web_client = None
        
        if self.config.slack_webhook_url:
            self.webhook_client = WebhookClient(self.config.slack_webhook_url)
        
        if self.config.slack_token:
            self.web_client = WebClient(token=self.config.slack_token)

    def send_failure_alert(self, failure: DAGFailure) -> bool:
        """Send a DAG failure alert to Slack.
        
        Args:
            failure: DAGFailure object with failure details
            
        Returns:
            True if alert was sent successfully
        """
        blocks = self._build_failure_blocks(failure)
        
        try:
            if self.webhook_client:
                response = self.webhook_client.send(
                    text=f"🚨 DAG Failed: {failure.dag_id}",
                    blocks=blocks,
                )
                return response.status_code == 200
            elif self.web_client and self.config.slack_channel:
                response = self.web_client.chat_postMessage(
                    channel=self.config.slack_channel,
                    text=f"🚨 DAG Failed: {failure.dag_id}",
                    blocks=blocks,
                )
                return response["ok"]
        except SlackApiError as e:
            logger.error(f"Failed to send Slack alert: {e}")
            return False
        
        logger.warning("No Slack client configured")
        return False

    def send_sla_miss_alert(self, sla_miss: SLAMissEvent) -> bool:
        """Send an SLA miss alert to Slack.
        
        Args:
            sla_miss: SLAMissEvent object with SLA miss details
            
        Returns:
            True if alert was sent successfully
        """
        blocks = self._build_sla_miss_blocks(sla_miss)
        
        try:
            if self.webhook_client:
                response = self.webhook_client.send(
                    text=f"⏰ SLA Miss: {sla_miss.dag_id}/{sla_miss.task_id}",
                    blocks=blocks,
                )
                return response.status_code == 200
            elif self.web_client and self.config.slack_channel:
                response = self.web_client.chat_postMessage(
                    channel=self.config.slack_channel,
                    text=f"⏰ SLA Miss: {sla_miss.dag_id}/{sla_miss.task_id}",
                    blocks=blocks,
                )
                return response["ok"]
        except SlackApiError as e:
            logger.error(f"Failed to send Slack alert: {e}")
            return False
        
        logger.warning("No Slack client configured")
        return False

    def send_batch_failure_summary(self, failures: List[DAGFailure]) -> bool:
        """Send a summary of multiple failures.
        
        Args:
            failures: List of DAGFailure objects
            
        Returns:
            True if summary was sent successfully
        """
        if not failures:
            return True
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🚨 {len(failures)} DAG Failures Summary",
                    "emoji": True,
                }
            },
            {"type": "divider"},
        ]
        
        for failure in failures[:10]:  # Limit to 10 to avoid message size limits
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*{failure.dag_id}*\n"
                        f"Run: `{failure.run_id}`\n"
                        f"Failed Tasks: {len(failure.failed_tasks)}"
                    ),
                }
            })
        
        if len(failures) > 10:
            blocks.append({
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"...and {len(failures) - 10} more failures",
                    }
                ]
            })
        
        try:
            if self.webhook_client:
                response = self.webhook_client.send(
                    text=f"🚨 {len(failures)} DAG Failures",
                    blocks=blocks,
                )
                return response.status_code == 200
            elif self.web_client and self.config.slack_channel:
                response = self.web_client.chat_postMessage(
                    channel=self.config.slack_channel,
                    text=f"🚨 {len(failures)} DAG Failures",
                    blocks=blocks,
                )
                return response["ok"]
        except SlackApiError as e:
            logger.error(f"Failed to send Slack summary: {e}")
            return False
        
        return False

    def _build_failure_blocks(self, failure: DAGFailure) -> list:
        """Build Slack blocks for a failure alert."""
        failed_tasks_text = "\n".join(
            [f"• `{t.task_id}` (attempt {t.try_number}/{t.max_tries})"
             for t in failure.failed_tasks[:5]]
        )
        if len(failure.failed_tasks) > 5:
            failed_tasks_text += f"\n...and {len(failure.failed_tasks) - 5} more"
        
        airflow_url = self.config.airflow_base_url.rstrip("/")
        dag_link = f"{airflow_url}/dags/{failure.dag_id}/grid"

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚨 DAG Failure Alert",
                    "emoji": True,
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n<{dag_link}|`{failure.dag_id}`>"},
                    {"type": "mrkdwn", "text": f"*Run ID:*\n`{failure.run_id}`"},
                    {"type": "mrkdwn", "text": f"*Execution Date:*\n{failure.execution_date}"},
                    {"type": "mrkdwn", "text": f"*Failed Tasks:*\n{len(failure.failed_tasks)}"},
                ]
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Failed Tasks:*\n{failed_tasks_text}" if failed_tasks_text else "*No task details available*",
                }
            },
        ]
        
        return blocks

    def _build_sla_miss_blocks(self, sla_miss: SLAMissEvent) -> list:
        """Build Slack blocks for an SLA miss alert."""
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "⏰ SLA Miss Alert",
                    "emoji": True,
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n`{sla_miss.dag_id}`"},
                    {"type": "mrkdwn", "text": f"*Task:*\n`{sla_miss.task_id}`"},
                    {"type": "mrkdwn", "text": f"*Execution Date:*\n{sla_miss.execution_date}"},
                    {"type": "mrkdwn", "text": f"*Timestamp:*\n{sla_miss.timestamp}"},
                ]
            },
        ]
        
        if sla_miss.description:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Description:*\n{sla_miss.description}",
                }
            })
        
        return blocks

    def send_threshold_alert(
        self,
        metric_name: str,
        current_value: float,
        threshold: float,
        severity: str = "warning",
    ) -> bool:
        """Send a threshold breach alert to Slack.
        
        Args:
            metric_name: Name of the metric
            current_value: Current metric value
            threshold: Threshold that was breached
            severity: Alert severity (info, warning, error, critical)
            
        Returns:
            True if alert was sent successfully
        """
        severity_emoji = {
            "info": "ℹ️",
            "warning": "⚠️",
            "error": "🚨",
            "critical": "🔥",
        }
        
        emoji = severity_emoji.get(severity, "⚠️")
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} Threshold Alert",
                    "emoji": True,
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Metric:*\n`{metric_name}`"},
                    {"type": "mrkdwn", "text": f"*Severity:*\n{severity.upper()}"},
                    {"type": "mrkdwn", "text": f"*Current Value:*\n{current_value}"},
                    {"type": "mrkdwn", "text": f"*Threshold:*\n{threshold}"},
                ]
            },
        ]
        
        try:
            if self.webhook_client:
                response = self.webhook_client.send(
                    text=f"{emoji} Threshold Alert: {metric_name} = {current_value}",
                    blocks=blocks,
                )
                return response.status_code == 200
            elif self.web_client and self.config.slack_channel:
                response = self.web_client.chat_postMessage(
                    channel=self.config.slack_channel,
                    text=f"{emoji} Threshold Alert: {metric_name} = {current_value}",
                    blocks=blocks,
                )
                return response["ok"]
        except SlackApiError as e:
            logger.error(f"Failed to send Slack threshold alert: {e}")
            return False
        
        return False

    def send_test_alert(self) -> bool:
        """Send a test alert to verify configuration.
        
        Returns:
            True if test alert was sent successfully
        """
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "✅ Airflow Watcher Test Alert",
                    "emoji": True,
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "This is a test alert from Airflow Watcher. If you see this, Slack notifications are configured correctly!",
                }
            },
        ]
        
        try:
            if self.webhook_client:
                response = self.webhook_client.send(
                    text="✅ Airflow Watcher Test Alert",
                    blocks=blocks,
                )
                return response.status_code == 200
            elif self.web_client and self.config.slack_channel:
                response = self.web_client.chat_postMessage(
                    channel=self.config.slack_channel,
                    text="✅ Airflow Watcher Test Alert",
                    blocks=blocks,
                )
                return response["ok"]
        except SlackApiError as e:
            logger.error(f"Failed to send Slack test alert: {e}")
            return False
        
        return False
