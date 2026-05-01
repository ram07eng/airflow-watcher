"""Backend factory for Airflow Watcher.

Supported values for ``AIRFLOW_WATCHER_BACKEND``
-------------------------------------------------

``airflow`` (default)
    Reads from Airflow's metadata DB via SQLAlchemy ORM.
    No extra dependencies.

``bigquery``
    Reads from a BigQuery table.
    Requires ``google-cloud-bigquery``
    (``pip install airflow-watcher[bigquery]``).
    Table structure is controlled by ``AIRFLOW_WATCHER_BQ_STRUCTURE``
    (``nested_array`` | ``flat``, default ``nested_array``).

``sqlalchemy``
    Reads from any SQLAlchemy-compatible DB (PostgreSQL, MySQL, SQLite).
    Only supports ``flat`` table structure.
    Requires ``AIRFLOW_WATCHER_EXTERNAL_DB_URI``.

Schema / column overrides (all backends except ``airflow``)
-----------------------------------------------------------
Set ``AIRFLOW_WATCHER_SCHEMA_JSON`` to a JSON object mapping logical
field names to actual column names::

    export AIRFLOW_WATCHER_SCHEMA_JSON='{
        "col_dag_id": "pipeline_id",
        "col_elapsed_time": "duration_seconds"
    }'

All keys are optional; unspecified fields use their defaults.

Factory returns
---------------
``None``
    Use the standard Airflow monitors (``backend=airflow``).
``ExternalBackend``
    Wraps the appropriate client for BQ or SQL backends.

Raises
------
``ValueError``
    Missing required config (e.g. no ``bq_table`` for BigQuery).
``ImportError``
    Required package not installed.
"""

from __future__ import annotations

import os
from typing import Optional


def get_backend(config) -> Optional[object]:
    """Return an ``ExternalBackend``, or ``None`` for the default Airflow monitors.

    Args:
        config: Object with attributes ``backend``, ``bq_table``,
                and optionally ``external_db_uri``.

    Returns:
        ``ExternalBackend`` instance, or ``None`` when
        ``backend='airflow'`` / any unrecognised value.
    """
    backend_type = getattr(config, "backend", "airflow")

    if backend_type == "bigquery":
        return _make_bigquery_backend(config)

    if backend_type == "sqlalchemy":
        return _make_sqlalchemy_backend(config)

    # 'airflow' or unrecognised → existing Airflow monitors unchanged
    return None


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------


def _build_schema(config, dialect: str, structure: str):
    """Build a SchemaConfig from config + env overrides."""
    from airflow_watcher.backends.schema_config import SchemaConfig

    table = getattr(config, "bq_table", "") or ""
    return SchemaConfig.from_env(table=table, dialect=dialect, structure=structure)


def _make_bigquery_backend(config):
    """Instantiate ExternalBackend backed by BQClient."""
    from airflow_watcher.backends.bigquery_backend import ExternalBackend
    from airflow_watcher.backends._bq_client import BQClient

    bq_table = getattr(config, "bq_table", None)
    if not bq_table:
        raise ValueError(
            "AIRFLOW_WATCHER_BQ_TABLE is required when "
            "AIRFLOW_WATCHER_BACKEND=bigquery. "
            "Set it to the fully-qualified table name:  project.dataset.table"
        )

    structure = getattr(config, "bq_structure", None) or os.environ.get("AIRFLOW_WATCHER_BQ_STRUCTURE", "nested_array")
    schema = _build_schema(config, dialect="bigquery", structure=structure)
    schema.table = bq_table  # ensure table is set

    client = BQClient(schema=schema)
    return ExternalBackend(client=client, backend_label="bigquery")


def _make_sqlalchemy_backend(config):
    """Instantiate ExternalBackend backed by SQLClient."""
    from airflow_watcher.backends.bigquery_backend import ExternalBackend
    from airflow_watcher.backends._sql_client import SQLClient

    db_uri = getattr(config, "external_db_uri", None) or os.environ.get("AIRFLOW_WATCHER_EXTERNAL_DB_URI", "")
    if not db_uri:
        raise ValueError(
            "AIRFLOW_WATCHER_EXTERNAL_DB_URI is required when "
            "AIRFLOW_WATCHER_BACKEND=sqlalchemy. "
            "Set it to a SQLAlchemy connection string:  postgresql://user:pass@host/db"
        )

    bq_table = getattr(config, "bq_table", None) or os.environ.get("AIRFLOW_WATCHER_BQ_TABLE", "")
    if not bq_table:
        raise ValueError(
            "AIRFLOW_WATCHER_BQ_TABLE (table name) is required when "
            "AIRFLOW_WATCHER_BACKEND=sqlalchemy. "
            "Set it to the target table name."
        )

    schema = _build_schema(config, dialect="sqlalchemy", structure="flat")
    schema.table = bq_table

    client = SQLClient(db_uri=db_uri, schema=schema)
    return ExternalBackend(client=client, backend_label="sqlalchemy")
