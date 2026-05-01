"""Schema configuration for external backends.

Decouples the backend SQL from a fixed table structure.  Users override
column names and table layout via ``AIRFLOW_WATCHER_SCHEMA_JSON``.

Supported structures
--------------------
``nested_array``
    BigQuery-specific.  Results are stored as an ``ARRAY<STRUCT<...>>``
    column (the dbt callback schema).  Queries use ``UNNEST``.

``flat``
    One row per task run (or per model run).  Works with BigQuery *and*
    any SQLAlchemy-supported database (PostgreSQL, MySQL, SQLite).

Quick-start: default values match the dbt callback morpheusdbtresult schema.
Override only the fields that differ from your table.

Example
-------
``AIRFLOW_WATCHER_SCHEMA_JSON`` overrides specific column names::

    export AIRFLOW_WATCHER_SCHEMA_JSON='{
        "col_dag_id": "pipeline_id",
        "col_elapsed_time": "duration_seconds",
        "col_state": "run_state"
    }'
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SchemaConfig:
    """Column-level schema configuration for an external data source.

    All ``col_*`` attributes map a *logical* field name used internally
    to the *actual* column name in the target table.

    Args:
        table: Fully-qualified table name.
               BQ example: ``project.dataset.table``
               SQL example: ``schema.table`` or just ``table``
        dialect: Query dialect.  ``bigquery`` uses BQ-specific syntax
                 (UNNEST, TIMESTAMP_SUB, named ``@param`` binds).
                 Any other value falls back to standard SQL with
                 SQLAlchemy ``:param`` binds.
        structure: ``nested_array`` — results are an ``ARRAY<STRUCT>``
                   column (dbt callback schema, BQ only).
                   ``flat`` — one row per task/model run (all dialects).
    """

    table: str = ""
    dialect: str = "bigquery"
    structure: str = "nested_array"  # "nested_array" | "flat"

    # ------------------------------------------------------------------
    # Core columns — present in both structures
    # ------------------------------------------------------------------
    col_dag_id: str = "dag_id"
    col_task_id: str = "task_id"
    col_run_id: str = "run_id"
    col_execution_date: str = "execution_date"
    col_elapsed_time: str = "elapsed_time"

    # ``state`` / ``status`` column used in flat structure to filter failures
    col_state: str = "state"

    # ------------------------------------------------------------------
    # nested_array structure — fields *inside* the results ARRAY<STRUCT>
    # ------------------------------------------------------------------

    # Name of the top-level ARRAY column
    col_results_array: str = "results"

    # Sub-fields accessed as ``r.<field>`` inside UNNEST
    col_model_id: str = "unique_id"
    col_model_status: str = "status"
    col_model_failures: str = "failures"
    col_model_message: str = "message"
    col_model_execution_time: str = "execution_time"

    # Nested paths for adapter_response (BQ dot-path notation)
    col_bytes_billed: str = "adapter_response.bytes_billed"
    col_slot_ms: str = "adapter_response.slot_ms"

    # Timing sub-array inside each result struct
    col_timing_array: str = "timing"
    col_timing_name: str = "name"
    col_timing_started: str = "started_at"
    col_timing_completed: str = "completed_at"

    # ------------------------------------------------------------------
    # flat structure — top-level columns (one row per model/task run)
    # ------------------------------------------------------------------

    col_model_id_flat: str = "model_id"
    col_model_status_flat: str = "model_status"
    col_model_failures_flat: str = "failure_count"
    col_model_message_flat: str = "message"
    col_model_execution_time_flat: str = "model_execution_time"
    col_bytes_billed_flat: str = "bytes_billed"
    col_slot_ms_flat: str = "slot_ms"

    # ------------------------------------------------------------------
    # Failure filter expressions — override for non-standard status values
    # ------------------------------------------------------------------

    # Value(s) in col_model_status / col_state that represent failure
    failure_status_value: str = "error"
    # Set to empty string to disable the failures > 0 guard
    failure_count_guard: str = "failures > 0"

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_env(
        cls,
        table: str = "",
        dialect: str = "bigquery",
        structure: str = "nested_array",
    ) -> "SchemaConfig":
        """Build from defaults + ``AIRFLOW_WATCHER_SCHEMA_JSON`` overrides.

        Only keys present in ``AIRFLOW_WATCHER_SCHEMA_JSON`` are overridden;
        all others keep their defaults.

        Args:
            table: Table name — used as default if not present in JSON.
            dialect: Dialect default.
            structure: Structure default.

        Returns:
            Populated ``SchemaConfig`` instance.

        Raises:
            ValueError: If ``AIRFLOW_WATCHER_SCHEMA_JSON`` is not valid JSON.
        """
        config = cls(table=table, dialect=dialect, structure=structure)

        raw = os.environ.get("AIRFLOW_WATCHER_SCHEMA_JSON", "").strip()
        if not raw:
            return config

        try:
            overrides: dict = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"AIRFLOW_WATCHER_SCHEMA_JSON is not valid JSON: {exc}") from exc

        unknown = []
        for key, value in overrides.items():
            if hasattr(config, key):
                setattr(config, key, value)
            else:
                unknown.append(key)

        if unknown:
            logger.warning(
                "AIRFLOW_WATCHER_SCHEMA_JSON contains unknown keys (ignored): %s",
                ", ".join(unknown),
            )

        return config

    # ------------------------------------------------------------------
    # Helpers used by query builders
    # ------------------------------------------------------------------

    def failure_filter_nested(self, alias: str = "r") -> str:
        """BQ WHERE clause fragment for nested_array failure filter."""
        parts = [f"{alias}.{self.col_model_status} = '{self.failure_status_value}'"]
        if self.failure_count_guard:
            guard = self.failure_count_guard.replace("failures", f"{alias}.{self.col_model_failures}")
            parts.append(guard)
        return f"({' OR '.join(parts)})"

    def failure_filter_flat(self) -> str:
        """SQL WHERE clause fragment for flat table failure filter."""
        parts = [f"{self.col_model_status_flat} = '{self.failure_status_value}'"]
        if self.failure_count_guard:
            guard = self.failure_count_guard.replace("failures", self.col_model_failures_flat)
            parts.append(guard)
        return f"({' OR '.join(parts)})"
