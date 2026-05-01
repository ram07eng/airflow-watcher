"""BigQuery client — schema-configurable via SchemaConfig.

Supports two table structures:

``nested_array``
    dbt callback schema: top-level row per task run; results are an
    ``ARRAY<STRUCT<...>>`` column.  Uses ``UNNEST``, BQ named params.

``flat``
    One row per model/task run.  Compatible with BQ flat exports or
    any BQ table that doesn't use nested arrays.  Still uses BQ
    query syntax (``@param``, ``TIMESTAMP_SUB``, etc.).

Column names are driven entirely by the injected ``SchemaConfig``.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from airflow_watcher.backends.schema_config import SchemaConfig

logger = logging.getLogger(__name__)


class BQClient:
    """BigQuery read-only client, schema-aware."""

    def __init__(self, schema: SchemaConfig) -> None:
        try:
            from google.cloud import bigquery as _bq
        except ImportError as exc:
            raise ImportError(
                "google-cloud-bigquery is required for the BigQuery backend. "
                "Install it with:  pip install 'airflow-watcher[bigquery]'"
            ) from exc
        self._bq = _bq
        self._client = _bq.Client()
        self._s = schema

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run(
        self,
        sql: str,
        params: Optional[list] = None,
    ) -> List[Dict[str, Any]]:
        job_config = self._bq.QueryJobConfig(query_parameters=params or [])
        rows = self._client.query(sql, job_config=job_config).result()
        return [dict(row) for row in rows]

    def _p(self, name: str, type_: str, value: Any):
        """Shorthand for ScalarQueryParameter."""
        return self._bq.ScalarQueryParameter(name, type_, value)

    def _cutoff_expr(self, param_name: str = "hours") -> str:
        """BQ expression for the start of the lookback window."""
        return f"TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @{param_name} HOUR)"

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
        params = [
            self._p("hours", "INT64", lookback_hours),
            self._p("lim", "INT64", limit),
            self._p("off", "INT64", offset),
        ]

        if s.structure == "nested_array":
            where = f"WHERE {s.col_execution_date} >= {self._cutoff_expr()}"
            if dag_id:
                where += f" AND {s.col_dag_id} = @dag_id"
                params.append(self._p("dag_id", "STRING", dag_id))
            fail_filter = s.failure_filter_nested()
            sql = f"""
            SELECT
                {s.col_dag_id}  AS dag_id,
                {s.col_task_id} AS task_id,
                {s.col_run_id}  AS run_id,
                {s.col_execution_date} AS execution_date,
                {s.col_elapsed_time}   AS elapsed_time,
                r.{s.col_model_id}              AS model_unique_id,
                r.{s.col_model_status}          AS model_status,
                r.{s.col_model_message}         AS model_message,
                r.{s.col_model_execution_time}  AS model_execution_time,
                r.{s.col_model_failures}        AS model_failure_count
            FROM `{s.table}`,
                 UNNEST({s.col_results_array}) AS r
            {where}
              AND {fail_filter}
            ORDER BY {s.col_execution_date} DESC
            LIMIT @lim OFFSET @off
            """
        else:
            where = f"WHERE {s.col_execution_date} >= {self._cutoff_expr()}"
            if dag_id:
                where += f" AND {s.col_dag_id} = @dag_id"
                params.append(self._p("dag_id", "STRING", dag_id))
            fail_filter = s.failure_filter_flat()
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
            FROM `{s.table}`
            {where}
              AND {fail_filter}
            ORDER BY {s.col_execution_date} DESC
            LIMIT @lim OFFSET @off
            """
        return self._run(sql, params)

    def get_failure_statistics(self, *, lookback_hours: int = 24) -> Dict[str, Any]:
        s = self._s
        params = [self._p("hours", "INT64", lookback_hours)]
        cutoff = self._cutoff_expr()

        if s.structure == "nested_array":
            fail_filter = s.failure_filter_nested()
            sql = f"""
            WITH task_runs AS (
                SELECT
                    {s.col_dag_id} AS dag_id,
                    {s.col_task_id} AS task_id,
                    {s.col_run_id} AS run_id,
                    ARRAY_LENGTH({s.col_results_array}) AS total_models,
                    (SELECT COUNTIF({fail_filter.replace("(", "").replace(")", "")})
                     FROM UNNEST({s.col_results_array}) r) AS failed_models
                FROM `{s.table}`
                WHERE {s.col_execution_date} >= {cutoff}
            )
            SELECT
                COUNT(*) AS total_task_runs,
                COUNTIF(failed_models > 0) AS task_runs_with_failures,
                SAFE_DIVIDE(COUNTIF(failed_models > 0), COUNT(*)) AS failure_rate,
                SUM(total_models) AS total_dbt_models_executed,
                SUM(failed_models) AS total_dbt_models_failed,
                COUNT(DISTINCT dag_id) AS unique_dags
            FROM task_runs
            """
            top_sql = f"""
            SELECT
                {s.col_dag_id} AS dag_id,
                COUNTIF(failed_models > 0) AS failure_count,
                COUNT(*) AS total_runs,
                SAFE_DIVIDE(COUNTIF(failed_models > 0), COUNT(*)) AS failure_rate
            FROM (
                SELECT
                    {s.col_dag_id},
                    {s.col_run_id},
                    (SELECT COUNTIF({fail_filter.replace("(", "").replace(")", "")})
                     FROM UNNEST({s.col_results_array}) r) AS failed_models
                FROM `{s.table}`
                WHERE {s.col_execution_date} >= {cutoff}
            )
            GROUP BY {s.col_dag_id}
            HAVING failure_count > 0
            ORDER BY failure_count DESC
            LIMIT 10
            """
        else:
            fail_filter = s.failure_filter_flat()
            sql = f"""
            SELECT
                COUNT(*) AS total_task_runs,
                COUNTIF({fail_filter}) AS task_runs_with_failures,
                SAFE_DIVIDE(COUNTIF({fail_filter}), COUNT(*)) AS failure_rate,
                COUNT(*) AS total_dbt_models_executed,
                COUNTIF({fail_filter}) AS total_dbt_models_failed,
                COUNT(DISTINCT {s.col_dag_id}) AS unique_dags
            FROM `{s.table}`
            WHERE {s.col_execution_date} >= {cutoff}
            """
            top_sql = f"""
            SELECT
                {s.col_dag_id} AS dag_id,
                COUNTIF({fail_filter}) AS failure_count,
                COUNT(*) AS total_runs,
                SAFE_DIVIDE(COUNTIF({fail_filter}), COUNT(*)) AS failure_rate
            FROM `{s.table}`
            WHERE {s.col_execution_date} >= {cutoff}
            GROUP BY {s.col_dag_id}
            HAVING failure_count > 0
            ORDER BY failure_count DESC
            LIMIT 10
            """

        rows = self._run(sql, params)
        stats = rows[0] if rows else {}
        stats["most_failing_dags"] = self._run(top_sql, params)
        return stats

    # ------------------------------------------------------------------
    # 2. Task health
    # ------------------------------------------------------------------

    def get_long_running_tasks(self, *, threshold_minutes: int = 60) -> List[Dict[str, Any]]:
        s = self._s
        params = [self._p("threshold_sec", "FLOAT64", threshold_minutes * 60.0)]
        cutoff = self._cutoff_expr("fixed_hours")
        # Use 48 h lookback for long-running tasks (no user param here)
        params.append(self._p("fixed_hours", "INT64", 48))

        if s.structure == "nested_array":
            sql = f"""
            SELECT
                {s.col_dag_id}  AS dag_id,
                {s.col_task_id} AS task_id,
                {s.col_run_id}  AS run_id,
                {s.col_execution_date} AS execution_date,
                {s.col_elapsed_time}   AS elapsed_time,
                ROUND({s.col_elapsed_time} / 60, 2) AS elapsed_minutes,
                ARRAY_LENGTH({s.col_results_array}) AS models_executed,
                (SELECT STRING_AGG(r.{s.col_model_id}, ', '
                         ORDER BY r.{s.col_model_execution_time} DESC LIMIT 3)
                 FROM UNNEST({s.col_results_array}) r) AS slowest_models
            FROM `{s.table}`
            WHERE {s.col_elapsed_time} > @threshold_sec
              AND {s.col_execution_date} >= {cutoff}
            ORDER BY {s.col_elapsed_time} DESC
            LIMIT 100
            """
        else:
            sql = f"""
            SELECT
                {s.col_dag_id}  AS dag_id,
                {s.col_task_id} AS task_id,
                {s.col_run_id}  AS run_id,
                {s.col_execution_date} AS execution_date,
                {s.col_elapsed_time}   AS elapsed_time,
                ROUND({s.col_elapsed_time} / 60, 2) AS elapsed_minutes,
                NULL AS models_executed,
                NULL AS slowest_models
            FROM `{s.table}`
            WHERE {s.col_elapsed_time} > @threshold_sec
              AND {s.col_execution_date} >= {cutoff}
            ORDER BY {s.col_elapsed_time} DESC
            LIMIT 100
            """
        return self._run(sql, params)

    def get_model_performance(self, *, lookback_hours: int = 24, top_n: int = 20) -> List[Dict[str, Any]]:
        s = self._s
        params = [
            self._p("hours", "INT64", lookback_hours),
            self._p("top_n", "INT64", top_n),
        ]
        cutoff = self._cutoff_expr()

        if s.structure == "nested_array":
            sql = f"""
            SELECT
                r.{s.col_model_id} AS model_id,
                COUNT(*) AS executions,
                ROUND(AVG(r.{s.col_model_execution_time}), 2) AS avg_seconds,
                ROUND(MAX(r.{s.col_model_execution_time}), 2) AS max_seconds,
                ROUND(MIN(r.{s.col_model_execution_time}), 2) AS min_seconds,
                COUNTIF({s.failure_filter_nested()}) AS failure_count,
                ROUND(AVG(r.{s.col_bytes_billed}) / POW(1024, 3), 4) AS avg_gb_billed,
                ROUND(AVG(r.{s.col_slot_ms}) / 1000, 2) AS avg_slot_seconds
            FROM `{s.table}`,
                 UNNEST({s.col_results_array}) AS r
            WHERE {s.col_execution_date} >= {cutoff}
            GROUP BY model_id
            ORDER BY avg_seconds DESC
            LIMIT @top_n
            """
        else:
            fail_filter = s.failure_filter_flat()
            sql = f"""
            SELECT
                {s.col_model_id_flat} AS model_id,
                COUNT(*) AS executions,
                ROUND(AVG({s.col_model_execution_time_flat}), 2) AS avg_seconds,
                ROUND(MAX({s.col_model_execution_time_flat}), 2) AS max_seconds,
                ROUND(MIN({s.col_model_execution_time_flat}), 2) AS min_seconds,
                COUNTIF({fail_filter}) AS failure_count,
                ROUND(AVG({s.col_bytes_billed_flat}) / POW(1024, 3), 4) AS avg_gb_billed,
                ROUND(AVG({s.col_slot_ms_flat}) / 1000, 2) AS avg_slot_seconds
            FROM `{s.table}`
            WHERE {s.col_execution_date} >= {cutoff}
            GROUP BY model_id
            ORDER BY avg_seconds DESC
            LIMIT @top_n
            """
        return self._run(sql, params)

    # ------------------------------------------------------------------
    # 3. Scheduling lag
    # ------------------------------------------------------------------

    def get_scheduling_lag(
        self,
        *,
        lookback_hours: int = 24,
        lag_threshold_minutes: int = 10,
    ) -> Dict[str, Any]:
        s = self._s
        params = [
            self._p("hours", "INT64", lookback_hours),
            self._p("threshold", "FLOAT64", lag_threshold_minutes),
        ]
        cutoff = self._cutoff_expr()

        if s.structure == "nested_array":
            ta = s.col_timing_array
            tn = s.col_timing_name
            ts = s.col_timing_started
            first_start = (
                f"(SELECT MIN(t.{ts}) FROM UNNEST({s.col_results_array}) r, UNNEST(r.{ta}) t WHERE t.{tn} = 'execute')"
            )
        else:
            # Flat: approximate lag using execution_date as proxy
            first_start = s.col_execution_date

        lag_expr = (
            f"TIMESTAMP_DIFF({first_start}, {s.col_execution_date}, SECOND) / 60.0"
            if s.structure == "nested_array"
            else "0.0"  # flat tables have no scheduling lag signal
        )

        sql = f"""
        WITH lag_data AS (
            SELECT
                {s.col_dag_id}  AS dag_id,
                {s.col_task_id} AS task_id,
                {s.col_run_id}  AS run_id,
                {s.col_execution_date} AS execution_date,
                {lag_expr} AS lag_minutes
            FROM `{s.table}`
            WHERE {s.col_execution_date} >= {cutoff}
        )
        SELECT
            ROUND(APPROX_QUANTILES(lag_minutes, 100)[OFFSET(50)], 2) AS p50_lag_minutes,
            ROUND(APPROX_QUANTILES(lag_minutes, 100)[OFFSET(90)], 2) AS p90_lag_minutes,
            ROUND(APPROX_QUANTILES(lag_minutes, 100)[OFFSET(95)], 2) AS p95_lag_minutes,
            ROUND(AVG(lag_minutes), 2) AS avg_lag_minutes,
            ROUND(MAX(lag_minutes), 2) AS max_lag_minutes,
            COUNT(*) AS total_measurements
        FROM lag_data
        WHERE lag_minutes IS NOT NULL
        """
        rows = self._run(sql, params)
        stats = rows[0] if rows else {}

        delayed_sql = f"""
        WITH lag_data AS (
            SELECT
                {s.col_dag_id}  AS dag_id,
                {s.col_task_id} AS task_id,
                {s.col_run_id}  AS run_id,
                {s.col_execution_date} AS execution_date,
                {lag_expr} AS lag_minutes
            FROM `{s.table}`
            WHERE {s.col_execution_date} >= {cutoff}
        )
        SELECT dag_id, task_id, run_id, execution_date, ROUND(lag_minutes, 2) AS lag_minutes
        FROM lag_data
        WHERE lag_minutes > @threshold
        ORDER BY lag_minutes DESC
        LIMIT 30
        """
        stats["delayed_dags"] = self._run(delayed_sql, params)
        return stats

    # ------------------------------------------------------------------
    # 4. DAG overview
    # ------------------------------------------------------------------

    def get_dag_overview(self, *, lookback_hours: int = 24) -> List[Dict[str, Any]]:
        s = self._s
        params = [self._p("hours", "INT64", lookback_hours)]
        cutoff = self._cutoff_expr()

        if s.structure == "nested_array":
            fail_filter = s.failure_filter_nested()
            sql = f"""
            SELECT
                {s.col_dag_id} AS dag_id,
                COUNT(DISTINCT {s.col_run_id}) AS total_runs,
                COUNT(DISTINCT {s.col_task_id}) AS unique_tasks,
                SUM(ARRAY_LENGTH({s.col_results_array})) AS total_dbt_models,
                SUM((SELECT COUNTIF({fail_filter.replace("(", "").replace(")", "")})
                     FROM UNNEST({s.col_results_array}) r)) AS failed_models,
                ROUND(AVG({s.col_elapsed_time}), 2) AS avg_elapsed_seconds,
                MAX({s.col_execution_date}) AS latest_execution,
                ROUND(SUM((SELECT SUM(r.{s.col_bytes_billed})
                           FROM UNNEST({s.col_results_array}) r)) / POW(1024, 3), 4)
                    AS total_gb_billed
            FROM `{s.table}`
            WHERE {s.col_execution_date} >= {cutoff}
            GROUP BY {s.col_dag_id}
            ORDER BY total_runs DESC
            """
        else:
            fail_filter = s.failure_filter_flat()
            sql = f"""
            SELECT
                {s.col_dag_id} AS dag_id,
                COUNT(DISTINCT {s.col_run_id}) AS total_runs,
                COUNT(DISTINCT {s.col_task_id}) AS unique_tasks,
                COUNT(*) AS total_dbt_models,
                COUNTIF({fail_filter}) AS failed_models,
                ROUND(AVG({s.col_elapsed_time}), 2) AS avg_elapsed_seconds,
                MAX({s.col_execution_date}) AS latest_execution,
                ROUND(SUM({s.col_bytes_billed_flat}) / POW(1024, 3), 4) AS total_gb_billed
            FROM `{s.table}`
            WHERE {s.col_execution_date} >= {cutoff}
            GROUP BY {s.col_dag_id}
            ORDER BY total_runs DESC
            """
        return self._run(sql, params)

    # ------------------------------------------------------------------
    # 5. Cost analysis
    # ------------------------------------------------------------------

    def get_cost_analysis(self, *, lookback_hours: int = 24) -> Dict[str, Any]:
        s = self._s
        params = [self._p("hours", "INT64", lookback_hours)]
        cutoff = self._cutoff_expr()

        if s.structure == "nested_array":
            sql = f"""
            WITH model_costs AS (
                SELECT
                    {s.col_dag_id} AS dag_id,
                    r.{s.col_model_id} AS model_id,
                    r.{s.col_bytes_billed} AS bytes_billed,
                    r.{s.col_slot_ms} AS slot_ms,
                    r.{s.col_model_execution_time} AS execution_time
                FROM `{s.table}`,
                     UNNEST({s.col_results_array}) AS r
                WHERE {s.col_execution_date} >= {cutoff}
                  AND r.{s.col_bytes_billed} IS NOT NULL
            )
            SELECT
                COUNT(*) AS total_model_runs,
                ROUND(SUM(bytes_billed) / POW(1024, 3), 4) AS total_gb_billed,
                ROUND(SUM(slot_ms) / 1000 / 3600, 2) AS total_slot_hours,
                ROUND(AVG(bytes_billed) / POW(1024, 3), 6) AS avg_gb_per_model,
                ROUND(AVG(execution_time), 2) AS avg_execution_seconds
            FROM model_costs
            """
            top_sql = f"""
            SELECT
                r.{s.col_model_id} AS model_id,
                {s.col_dag_id} AS dag_id,
                COUNT(*) AS executions,
                ROUND(SUM(r.{s.col_bytes_billed}) / POW(1024, 3), 4) AS total_gb_billed,
                ROUND(SUM(r.{s.col_slot_ms}) / 1000, 2) AS total_slot_seconds,
                ROUND(AVG(r.{s.col_model_execution_time}), 2) AS avg_seconds
            FROM `{s.table}`,
                 UNNEST({s.col_results_array}) AS r
            WHERE {s.col_execution_date} >= {cutoff}
              AND r.{s.col_bytes_billed} IS NOT NULL
            GROUP BY model_id, {s.col_dag_id}
            ORDER BY total_gb_billed DESC
            LIMIT 15
            """
        else:
            sql = f"""
            SELECT
                COUNT(*) AS total_model_runs,
                ROUND(SUM({s.col_bytes_billed_flat}) / POW(1024, 3), 4) AS total_gb_billed,
                ROUND(SUM({s.col_slot_ms_flat}) / 1000 / 3600, 2) AS total_slot_hours,
                ROUND(AVG({s.col_bytes_billed_flat}) / POW(1024, 3), 6) AS avg_gb_per_model,
                ROUND(AVG({s.col_model_execution_time_flat}), 2) AS avg_execution_seconds
            FROM `{s.table}`
            WHERE {s.col_execution_date} >= {cutoff}
              AND {s.col_bytes_billed_flat} IS NOT NULL
            """
            top_sql = f"""
            SELECT
                {s.col_model_id_flat} AS model_id,
                {s.col_dag_id} AS dag_id,
                COUNT(*) AS executions,
                ROUND(SUM({s.col_bytes_billed_flat}) / POW(1024, 3), 4) AS total_gb_billed,
                ROUND(SUM({s.col_slot_ms_flat}) / 1000, 2) AS total_slot_seconds,
                ROUND(AVG({s.col_model_execution_time_flat}), 2) AS avg_seconds
            FROM `{s.table}`
            WHERE {s.col_execution_date} >= {cutoff}
              AND {s.col_bytes_billed_flat} IS NOT NULL
            GROUP BY model_id, {s.col_dag_id}
            ORDER BY total_gb_billed DESC
            LIMIT 15
            """

        rows = self._run(sql, params)
        summary = rows[0] if rows else {}
        summary["top_cost_models"] = self._run(top_sql, params)
        return summary

    # ------------------------------------------------------------------
    # 6. Failure correlations
    # ------------------------------------------------------------------

    def get_failure_correlations(self, *, lookback_hours: int = 48) -> Dict[str, Any]:
        s = self._s
        params = [self._p("hours", "INT64", lookback_hours)]
        cutoff = self._cutoff_expr()

        if s.structure == "nested_array":
            fail_filter = s.failure_filter_nested()
            failed_cte = f"""
            SELECT DISTINCT
                {s.col_dag_id} AS dag_id,
                {s.col_run_id} AS run_id,
                {s.col_execution_date} AS execution_date,
                DATE({s.col_execution_date}) AS fail_date,
                EXTRACT(HOUR FROM {s.col_execution_date}) AS fail_hour
            FROM `{s.table}`,
                 UNNEST({s.col_results_array}) AS r
            WHERE {s.col_execution_date} >= {cutoff}
              AND {fail_filter}
            """
        else:
            fail_filter = s.failure_filter_flat()
            failed_cte = f"""
            SELECT DISTINCT
                {s.col_dag_id} AS dag_id,
                {s.col_run_id} AS run_id,
                {s.col_execution_date} AS execution_date,
                DATE({s.col_execution_date}) AS fail_date,
                EXTRACT(HOUR FROM {s.col_execution_date}) AS fail_hour
            FROM `{s.table}`
            WHERE {s.col_execution_date} >= {cutoff}
              AND {fail_filter}
            """

        sql = f"""
        WITH failed_runs AS ({failed_cte}),
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
        HAVING co_occurrences >= 2
        ORDER BY co_occurrences DESC
        LIMIT 20
        """
        correlations = self._run(sql, params)
        return {"correlations": correlations, "lookback_hours": lookback_hours}
