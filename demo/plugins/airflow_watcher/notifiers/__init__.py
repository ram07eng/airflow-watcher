"""Notifiers package for Airflow Watcher."""

from airflow_watcher.notifiers.slack_notifier import SlackNotifier
from airflow_watcher.notifiers.email_notifier import EmailNotifier

__all__ = ["SlackNotifier", "EmailNotifier"]
