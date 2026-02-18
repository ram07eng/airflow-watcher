"""Email Notifier for Airflow Watcher alerts."""

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional

from airflow_watcher.config import WatcherConfig
from airflow_watcher.models.failure import DAGFailure
from airflow_watcher.models.sla import SLAMissEvent

logger = logging.getLogger(__name__)


class EmailNotifier:
    """Sends alerts via email."""

    def __init__(self, config: Optional[WatcherConfig] = None):
        """Initialize Email notifier.

        Args:
            config: Configuration object with email settings
        """
        self.config = config or WatcherConfig()

    def send_failure_alert(self, failure: DAGFailure, recipients: Optional[List[str]] = None) -> bool:
        """Send a DAG failure alert via email.

        Args:
            failure: DAGFailure object with failure details
            recipients: List of email recipients (overrides config)

        Returns:
            True if email was sent successfully
        """
        to_addresses = recipients or self.config.email_recipients
        if not to_addresses:
            logger.warning("No email recipients configured")
            return False

        subject = f"🚨 DAG Failure: {failure.dag_id}"
        html_body = self._build_failure_html(failure)

        return self._send_email(subject, html_body, to_addresses)

    def send_sla_miss_alert(self, sla_miss: SLAMissEvent, recipients: Optional[List[str]] = None) -> bool:
        """Send an SLA miss alert via email.

        Args:
            sla_miss: SLAMissEvent object with SLA miss details
            recipients: List of email recipients (overrides config)

        Returns:
            True if email was sent successfully
        """
        to_addresses = recipients or self.config.email_recipients
        if not to_addresses:
            logger.warning("No email recipients configured")
            return False

        subject = f"⏰ SLA Miss: {sla_miss.dag_id}/{sla_miss.task_id}"
        html_body = self._build_sla_miss_html(sla_miss)

        return self._send_email(subject, html_body, to_addresses)

    def send_daily_summary(
        self,
        failures: List[DAGFailure],
        sla_misses: List[SLAMissEvent],
        recipients: Optional[List[str]] = None,
    ) -> bool:
        """Send a daily summary email.

        Args:
            failures: List of DAG failures
            sla_misses: List of SLA misses
            recipients: List of email recipients

        Returns:
            True if email was sent successfully
        """
        to_addresses = recipients or self.config.email_recipients
        if not to_addresses:
            logger.warning("No email recipients configured")
            return False

        subject = f"📊 Airflow Daily Summary: {len(failures)} failures, {len(sla_misses)} SLA misses"
        html_body = self._build_summary_html(failures, sla_misses)

        return self._send_email(subject, html_body, to_addresses)

    def _send_email(self, subject: str, html_body: str, recipients: List[str]) -> bool:
        """Send an email.

        Args:
            subject: Email subject
            html_body: HTML body content
            recipients: List of recipient email addresses

        Returns:
            True if email was sent successfully
        """
        if not self.config.smtp_host:
            logger.warning("SMTP host not configured")
            return False

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self.config.email_from
            msg["To"] = ", ".join(recipients)

            html_part = MIMEText(html_body, "html")
            msg.attach(html_part)

            with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as server:
                if self.config.smtp_use_tls:
                    server.starttls()
                if self.config.smtp_user and self.config.smtp_password:
                    server.login(self.config.smtp_user, self.config.smtp_password)
                server.sendmail(self.config.email_from, recipients, msg.as_string())

            logger.info(f"Email sent to {recipients}")
            return True

        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False

    def _build_failure_html(self, failure: DAGFailure) -> str:
        """Build HTML content for a failure alert."""
        failed_tasks_html = "".join(
            f"<li><code>{t.task_id}</code> (attempt {t.try_number}/{t.max_tries})</li>"
            for t in failure.failed_tasks
        )

        return f"""
        <html>
        <body style="font-family: Arial, sans-serif; padding: 20px;">
            <h2 style="color: #d32f2f;">🚨 DAG Failure Alert</h2>
            <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">DAG ID</td>
                    <td style="padding: 8px; border: 1px solid #ddd;"><code>{failure.dag_id}</code></td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Run ID</td>
                    <td style="padding: 8px; border: 1px solid #ddd;"><code>{failure.run_id}</code></td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Execution Date</td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{failure.execution_date}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Start Time</td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{failure.start_date}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">End Time</td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{failure.end_date}</td>
                </tr>
            </table>

            <h3>Failed Tasks ({len(failure.failed_tasks)})</h3>
            <ul>{failed_tasks_html}</ul>
        </body>
        </html>
        """

    def _build_sla_miss_html(self, sla_miss: SLAMissEvent) -> str:
        """Build HTML content for an SLA miss alert."""
        return f"""
        <html>
        <body style="font-family: Arial, sans-serif; padding: 20px;">
            <h2 style="color: #f57c00;">⏰ SLA Miss Alert</h2>
            <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">DAG ID</td>
                    <td style="padding: 8px; border: 1px solid #ddd;"><code>{sla_miss.dag_id}</code></td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Task ID</td>
                    <td style="padding: 8px; border: 1px solid #ddd;"><code>{sla_miss.task_id}</code></td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Execution Date</td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{sla_miss.execution_date}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Timestamp</td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{sla_miss.timestamp}</td>
                </tr>
            </table>

            {f"<p><strong>Description:</strong> {sla_miss.description}</p>" if sla_miss.description else ""}
        </body>
        </html>
        """

    def _build_summary_html(self, failures: List[DAGFailure], sla_misses: List[SLAMissEvent]) -> str:
        """Build HTML content for a daily summary."""
        failures_html = "".join(
            f"<tr><td style='padding: 4px;'><code>{f.dag_id}</code></td>"
            f"<td style='padding: 4px;'>{f.execution_date}</td>"
            f"<td style='padding: 4px;'>{len(f.failed_tasks)}</td></tr>"
            for f in failures[:20]
        )

        sla_html = "".join(
            f"<tr><td style='padding: 4px;'><code>{s.dag_id}</code></td>"
            f"<td style='padding: 4px;'><code>{s.task_id}</code></td>"
            f"<td style='padding: 4px;'>{s.timestamp}</td></tr>"
            for s in sla_misses[:20]
        )

        return f"""
        <html>
        <body style="font-family: Arial, sans-serif; padding: 20px;">
            <h2>📊 Airflow Daily Summary</h2>

            <h3 style="color: #d32f2f;">DAG Failures ({len(failures)})</h3>
            <table style="border-collapse: collapse; width: 100%;">
                <tr style="background: #f5f5f5;">
                    <th style="padding: 8px; text-align: left;">DAG</th>
                    <th style="padding: 8px; text-align: left;">Execution Date</th>
                    <th style="padding: 8px; text-align: left;">Failed Tasks</th>
                </tr>
                {failures_html}
            </table>
            {f"<p>...and {len(failures) - 20} more</p>" if len(failures) > 20 else ""}

            <h3 style="color: #f57c00;">SLA Misses ({len(sla_misses)})</h3>
            <table style="border-collapse: collapse; width: 100%;">
                <tr style="background: #f5f5f5;">
                    <th style="padding: 8px; text-align: left;">DAG</th>
                    <th style="padding: 8px; text-align: left;">Task</th>
                    <th style="padding: 8px; text-align: left;">Timestamp</th>
                </tr>
                {sla_html}
            </table>
            {f"<p>...and {len(sla_misses) - 20} more</p>" if len(sla_misses) > 20 else ""}
        </body>
        </html>
        """
