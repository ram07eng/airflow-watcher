"""BigQuery client — re-exported from the main package.

The canonical implementation lives in
``airflow_watcher.backends._bq_client``.  This shim keeps backwards
compatibility for code that imported directly from the demo directory.
"""

from airflow_watcher.backends._bq_client import BQClient  # noqa: F401

__all__ = ["BQClient"]
