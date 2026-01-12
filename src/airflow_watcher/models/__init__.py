"""Models package for Airflow Watcher."""

from airflow_watcher.models.failure import DAGFailure, TaskFailure
from airflow_watcher.models.sla import SLAMissEvent, SLADelayWarning

__all__ = ["DAGFailure", "TaskFailure", "SLAMissEvent", "SLADelayWarning"]
