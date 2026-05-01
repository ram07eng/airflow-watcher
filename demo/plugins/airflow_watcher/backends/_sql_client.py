"""SQLAlchemy-based external backend client.

Supports flat-table schemas on any SQLAlchemy-compatible database
(PostgreSQL, MySQL, SQLite, etc.).

Uses Python-computed cutoff timestamps as bind parameters so that
date arithmetic works across all dialects without dialect-specific
SQL functions.

Column names and table name are driven entirely by the injected
``SchemaConfig``.  Only ``structure='flat'`` is supported — nested
ARRAY schemas require BigQuery.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from airflow_watcher.backends.schema_config import SchemaConfig

logger = logging.getLogger(__name__)


class SQLClient:
    """Read-only SQLAlchemy client for flat external tables.

    Args:
        db_uri: SQLAlchemy connection string
                (e.g. ``postgresql://user:pass@host/db``).
        schema: ``SchemaConfig`` instance.  Must have
                ``structure='flat'``.

    Raises:
        ValueError: If ``schema.structure != 'flat'``.
        ImportError: If SQLAlchemy is not installed.
    """

    def __init__(self, db_uri: str, schema: SchemaConfig) -> None:
        if schema.structure != "flat":
            raise ValueError(
                f"SQLClient only supports structure='flat'; "
                f"got structure='{schema.structure}'. "
                "Use BQClient for nested_array tables."
            )
        try:
            from sqlalchemy import create_engine, text as sa_text

            self._text = sa_text
        except ImportError as exc:
            raise ImportError(
                "sqlalchemy is required for the sqlalchemy backend. "
                "Install it with:  pip install 'airflow-watcher[standalone]'"
            ) from exc

        self._engine = create_engine(db_uri, pool_pre_ping=True)
        self._s = schema

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        with self._engine.connect() as conn:
            result = conn.execute(self._text(sql), params or {})
            return [dict(row._mapping) for row in result]

    def _cutoff(self, lookback_hours: int) -> datetime:
        """Return naive UTC cutoff timestamp for the lookback window.

        Naive datetime is used for broad SQLite compatibility.
        PostgreSQL / MySQL accept naive UTC datetimes when the column
        type is TIMESTAMP WITHOUT TIME ZONE (the common default).
        """
        return datetime.utcnow() - timedelta(hours=lookback_hours)

    # ------------------------------------------------------------------
    # 1. Failures
    # ------------------------------------------------------------------

    def get_recent_failures(
        self,
        *,
        dag_id: Optional[str] = None,
        lookback_hours: int = 24,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        s = self._s
        fail_filter = s.failure_filter_flat()
        where = f"{s.col_execution_date} >= :cutoff AND {fail_filter}"
        params: Dict[str, Any] = {"cutoff": self._cutoff(lookback_hours), "lim": limit, "off": offset}

        if dag_id:
            where += f" AND {s.col_dag_id} = :dag_id"
            params["dag_id"] = dag_id

        sql = f"""
        SELECT
            {s.col_dag_id}  AS dag_id,
            {s.col_task_id} AS task_id,
            {s.col_run_id}  AS run_id,
            {s.col_execution_date} AS execution_date,
            {s.col_elapsed_time}   AS elapsed_time,
            {s.col_model_id_flat}              AS model_unique_id,
            {s.col_model_status_flat}          AS model_status,
            {s.col_model_message_flat}         AS model_message,
            {s.col_model_execution_time_flat}  AS model_execution_time,
            {s.col_model_failures_flat}        AS model_failure_count
        FROM {s.table}
        WHERE {where}
        ORDER BY {s.col_execution_date} DESC
        LIMIT :lim OFFSET :off
        """
        return self._run(sql, params)

    def get_failure_statistics(self, *, lookback_hours: int = 24) -> Dict[str, Any]:
        s = self._s
        fail_filter = s.failure_filter_flat()
        params: Dict[str, Any] = {"cutoff": self._cutoff(lookback_hours)}

        sql = f"""
        SELECT
            COUNT(*) AS total_task_runs,
            SUM(CASE WHEN {fail_filter} THEN 1 ELSE 0 END) AS task_runs_with_failures,
            CAST(SUM(CASE WHEN {fail_filter} THEN 1 ELSE 0 END) AS FLOAT)
                / NULLIF(COUNT(*), 0) AS failure_rate,
            COUNT(*) AS total_dbt_models_executed,
            SUM(CASE WHEN {fail_filter} THEN 1 ELSE 0 END) AS total_dbt_models_failed,
            COUNT(DISTINCT {s.col_dag_id}) AS unique_dags
        FROM {s.table}
        WHERE {s.col_execution_date} >= :cutoff
        """
        rows = self._run(sql, params)
        stats: Dict[str, Any] = rows[0] if rows else {}

        top_sql = f"""
        SELECT
            {s.col_dag_id} AS dag_id,
            SUM(CASE WHEN {fail_filter} THEN 1 ELSE 0 END) AS failure_count,
            COUNT(*) AS total_runs,
            CAST(SUM(CASE WHEN {fail_filter} THEN 1 ELSE 0 END) AS FLOAT)
                / NULLIF(COUNT(*), 0) AS failure_rate
        FROM {s.table}
        WHERE {s.col_execution_date} >= :cutoff
        GROUP BY {s.col_dag_id}
        HAVING SUM(CASE WHEN {fail_filter} THEN 1 ELSE 0 END) > 0
        ORDER BY failure_count DESC
        LIMIT 10
        """
        stats["most_failing_dags"] = self._run(top_sql, params)
        return stats

    # ------------------------------------------------------------------
    # 2. Task health
    # ------------------------------------------------------------------

    def get_long_running_tasks(self, *, threshold_minutes: int = 60) -> List[Dict[str, Any]]:
        s = self._s
        params = {
            "cutoff": self._cutoff(48),
            "threshold_sec": threshold_minutes * 60.0,
        }
        sql = f"""
        SELECT
            {s.col_dag_id}  AS dag_id,
            {s.col_task_id} AS task_id,
            {s.col_run_id}  AS run_id,
            {s.col_execution_date} AS execution_date,
            {s.col_elapsed_time}   AS elapsed_time,
            ROUND(CAST({s.col_elapsed_time} AS FLOAT) / 60, 2) AS elapsed_minutes,
            NULL AS models_executed,
            NULL AS slowest_models
        FROM {s.table}
        WHERE {s.col_elapsed_time} > :threshold_sec
          AND {s.col_execution_date} >= :cutoff
        ORDER BY {s.col_elapsed_time} DESC
        LIMIT 100
        """
        return self._run(sql, params)

    def get_model_performance(self, *, lookback_hours: int = 24, top_n: int = 20) -> List[Dict[str, Any]]:
        s = self._s
        fail_filter = s.failure_filter_flat()
        params = {"cutoff": self._cutoff(lookback_hours), "top_n": top_n}
        sql = f"""
        SELECT
            {s.col_model_id_flat} AS model_id,
            COUNT(*) AS executions,
            ROUND(AVG(CAST({s.col_model_execution_time_flat} AS FLOAT)), 2) AS avg_seconds,
            ROUND(MAX(CAST({s.col_model_execution_time_flat} AS FLOAT)), 2) AS max_seconds,
            ROUND(MIN(CAST({s.col_model_execution_time_flat} AS FLOAT)), 2) AS min_seconds,
            SUM(CASE WHEN {fail_filter} THEN 1 ELSE 0 END) AS failure_count,
            ROUND(AVG(CAST({s.col_bytes_billed_flat} AS FLOAT)) / POW(1024, 3), 4) AS avg_gb_billed,
            ROUND(AVG(CAST({s.col_slot_ms_flat} AS FLOAT)) / 1000, 2) AS avg_slot_seconds
        FROM {s.table}
        WHERE {s.col_execution_date} >= :cutoff
        GROUP BY {s.col_model_id_flat}
        ORDER BY avg_seconds DESC
        LIMIT :top_n
        """
        return self._run(sql, params)

    # ------------------------------------------------------------------
    # 3. Scheduling lag (flat tables have no timing sub-arrays)
    # ------------------------------------------------------------------

    def get_scheduling_lag(
        self,
        *,
        lookback_hours: int = 24,
        lag_threshold_minutes: int = 10,
    ) -> Dict[str, Any]:
        """Flat tables carry no scheduling lag signal — return zeros."""
        return {
            "p50_lag_minutes": None,
            "p90_lag_minutes": None,
            "p95_lag_minutes": None,
            "avg_lag_minutes": None,
            "max_lag_minutes": None,
            "total_measurements": 0,
            "delayed_dags": [],
            "backend_note": (
                "Scheduling lag is not available for flat-table SQL backends "
                "(no timing sub-array). Use nested_array BQ structure if needed."
            ),
        }

    # ------------------------------------------------------------------
    # 4. DAG overview
    # ------------------------------------------------------------------

    def get_dag_overview(self, *, lookback_hours: int = 24) -> List[Dict[str, Any]]:
        s = self._s
        fail_filter = s.failure_filter_flat()
        params = {"cutoff": self._cutoff(lookback_hours)}
        sql = f"""
        SELECT
            {s.col_dag_id} AS dag_id,
            COUNT(DISTINCT {s.col_run_id}) AS total_runs,
            COUNT(DISTINCT {s.col_task_id}) AS unique_tasks,
            COUNT(*) AS total_dbt_models,
            SUM(CASE WHEN {fail_filter} THEN 1 ELSE 0 END) AS failed_models,
            ROUND(AVG(CAST({s.col_elapsed_time} AS FLOAT)), 2) AS avg_elapsed_seconds,
            MAX({s.col_execution_date}) AS latest_execution,
            ROUND(SUM(CAST(COALESCE({s.col_bytes_billed_flat}, 0) AS FLOAT))
                  / POW(1024, 3), 4) AS total_gb_billed
        FROM {s.table}
        WHERE {s.col_execution_date} >= :cutoff
        GROUP BY {s.col_dag_id}
        ORDER BY total_runs DESC
        """
        return self._run(sql, params)

    # ------------------------------------------------------------------
    # 5. Cost analysis
    # ------------------------------------------------------------------

    def get_cost_analysis(self, *, lookback_hours: int = 24) -> Dict[str, Any]:
        s = self._s
        params = {"cutoff": self._cutoff(lookback_hours)}
        sql = f"""
        SELECT
            COUNT(*) AS total_model_runs,
            ROUND(SUM(CAST(COALESCE({s.col_bytes_billed_flat}, 0) AS FLOAT))
                  / POW(1024, 3), 4) AS total_gb_billed,
            ROUND(SUM(CAST(COALESCE({s.col_slot_ms_flat}, 0) AS FLOAT))
                  / 1000 / 3600, 2) AS total_slot_hours,
            ROUND(AVG(CAST(COALESCE({s.col_bytes_billed_flat}, 0) AS FLOAT))
                  / POW(1024, 3), 6) AS avg_gb_per_model,
            ROUND(AVG(CAST({s.col_model_execution_time_flat} AS FLOAT)), 2) AS avg_execution_seconds
        FROM {s.table}
        WHERE {s.col_execution_date} >= :cutoff
          AND {s.col_bytes_billed_flat} IS NOT NULL
        """
        rows = self._run(sql, params)
        summary: Dict[str, Any] = rows[0] if rows else {}

        top_sql = f"""
        SELECT
            {s.col_model_id_flat} AS model_id,
            {s.col_dag_id} AS dag_id,
            COUNT(*) AS executions,
            ROUND(SUM(CAST({s.col_bytes_billed_flat} AS FLOAT)) / POW(1024, 3), 4) AS total_gb_billed,
            ROUND(SUM(CAST({s.col_slot_ms_flat} AS FLOAT)) / 1000, 2) AS total_slot_seconds,
            ROUND(AVG(CAST({s.col_model_execution_time_flat} AS FLOAT)), 2) AS avg_seconds
        FROM {s.table}
        WHERE {s.col_execution_date} >= :cutoff
          AND {s.col_bytes_billed_flat} IS NOT NULL
        GROUP BY {s.col_model_id_flat}, {s.col_dag_id}
        ORDER BY total_gb_billed DESC
        LIMIT 15
        """
        summary["top_cost_models"] = self._run(top_sql, params)
        return summary

    # ------------------------------------------------------------------
    # 6. Failure correlations
    # ------------------------------------------------------------------

    def get_failure_correlations(self, *, lookback_hours: int = 48) -> Dict[str, Any]:
        s = self._s
        fail_filter = s.failure_filter_flat()
        params = {"cutoff": self._cutoff(lookback_hours)}

        # Co-occurrence within the same calendar day+hour.
        # SQLite lacks EXTRACT; use strftime instead.
        if s.dialect == "sqlite":
            date_expr = f"strftime('%Y-%m-%d', {s.col_execution_date})"
            hour_expr = f"CAST(strftime('%H', {s.col_execution_date}) AS INTEGER)"
        else:
            date_expr = f"CAST({s.col_execution_date} AS DATE)"
            hour_expr = f"EXTRACT(HOUR FROM {s.col_execution_date})"

        sql = f"""
        WITH failed_runs AS (
            SELECT DISTINCT
                {s.col_dag_id} AS dag_id,
                {date_expr} AS fail_date,
                {hour_expr} AS fail_hour
            FROM {s.table}
            WHERE {s.col_execution_date} >= :cutoff
              AND {fail_filter}
        ),
        pairs AS (
            SELECT
                a.dag_id AS dag_a,
                b.dag_id AS dag_b,
                a.fail_date,
                a.fail_hour
            FROM failed_runs a
            JOIN failed_runs b
              ON a.fail_date = b.fail_date
             AND a.fail_hour = b.fail_hour
             AND a.dag_id < b.dag_id
        )
        SELECT dag_a, dag_b, COUNT(*) AS co_occurrences
        FROM pairs
        GROUP BY dag_a, dag_b
        HAVING COUNT(*) >= 2
        ORDER BY co_occurrences DESC
        LIMIT 20
        """
        correlations = self._run(sql, params)
        return {"correlations": correlations, "lookback_hours": lookback_hours}
