"""BigQuery client for the airflow-watcher PoC.

Thin wrapper around ``google.cloud.bigquery.Client`` that issues
parameterised SQL against the morpheusdbtresult table and returns
plain dicts / lists ready for the API layer.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from google.cloud import bigquery

logger = logging.getLogger(__name__)

# Fully-qualified table names (overridable via env)
_DEFAULT_TABLE = "just-data.production_je_monitoring.morpheusdbtresult_all_2026"


class BQClient:
    """Lightweight BigQuery read-only client for the PoC."""

    def __init__(self, table: str = _DEFAULT_TABLE) -> None:
        self._client = bigquery.Client()
        self._table = table

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run(self, sql: str, params: Optional[List[bigquery.ScalarQueryParameter]] = None) -> List[Dict[str, Any]]:
        job_config = bigquery.QueryJobConfig(query_parameters=params or [])
        rows = self._client.query(sql, job_config=job_config).result()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # 1. Failures — recent DAG/task failures
    # ------------------------------------------------------------------

    def get_recent_failures(
        self,
        *,
        dag_id: Optional[str] = None,
        lookback_hours: int = 24,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Return recent dbt task runs that contain at least one failed model."""
        where = "WHERE execution_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)"
        params: list[bigquery.ScalarQueryParameter] = [
            bigquery.ScalarQueryParameter("hours", "INT64", lookback_hours),
            bigquery.ScalarQueryParameter("lim", "INT64", limit),
            bigquery.ScalarQueryParameter("off", "INT64", offset),
        ]
        if dag_id:
            where += " AND dag_id = @dag_id"
            params.append(bigquery.ScalarQueryParameter("dag_id", "STRING", dag_id))

        sql = f"""
        SELECT
            dag_id,
            task_id,
            run_id,
            execution_date,
            elapsed_time,
            r.unique_id   AS model_unique_id,
            r.status       AS model_status,
            r.message      AS model_message,
            r.execution_time AS model_execution_time,
            r.failures     AS model_failure_count
        FROM `{self._table}`,
             UNNEST(results) AS r
        {where}
          AND (r.status = 'error' OR r.failures > 0)
        ORDER BY execution_date DESC
        LIMIT @lim OFFSET @off
        """
        return self._run(sql, params)

    def get_failure_statistics(self, *, lookback_hours: int = 24) -> Dict[str, Any]:
        """Aggregate failure stats over the lookback window."""
        params = [bigquery.ScalarQueryParameter("hours", "INT64", lookback_hours)]
        sql = f"""
        WITH task_runs AS (
            SELECT
                dag_id,
                task_id,
                run_id,
                execution_date,
                ARRAY_LENGTH(results) AS total_models,
                (SELECT COUNTIF(r.status = 'error' OR r.failures > 0) FROM UNNEST(results) r) AS failed_models
            FROM `{self._table}`
            WHERE execution_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
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
        rows = self._run(sql, params)
        stats = rows[0] if rows else {}

        # Top failing DAGs
        top_sql = f"""
        SELECT
            dag_id,
            COUNTIF(failed_models > 0) AS failure_count,
            COUNT(*) AS total_runs,
            SAFE_DIVIDE(COUNTIF(failed_models > 0), COUNT(*)) AS failure_rate
        FROM (
            SELECT
                dag_id,
                run_id,
                (SELECT COUNTIF(r.status = 'error' OR r.failures > 0) FROM UNNEST(results) r) AS failed_models
            FROM `{self._table}`
            WHERE execution_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
        )
        GROUP BY dag_id
        HAVING failure_count > 0
        ORDER BY failure_count DESC
        LIMIT 10
        """
        top = self._run(top_sql, params)
        stats["most_failing_dags"] = top
        return stats

    # ------------------------------------------------------------------
    # 2. Task health — long-running tasks, dbt model performance
    # ------------------------------------------------------------------

    def get_long_running_tasks(self, *, threshold_minutes: int = 60) -> List[Dict[str, Any]]:
        """Tasks (dbt invocations) whose elapsed_time exceeds the threshold."""
        params = [bigquery.ScalarQueryParameter("threshold_sec", "FLOAT64", threshold_minutes * 60.0)]
        sql = f"""
        SELECT
            dag_id,
            task_id,
            run_id,
            execution_date,
            elapsed_time,
            ROUND(elapsed_time / 60, 2) AS elapsed_minutes,
            ARRAY_LENGTH(results) AS models_executed,
            (SELECT STRING_AGG(r.unique_id, ', ' ORDER BY r.execution_time DESC LIMIT 3)
             FROM UNNEST(results) r) AS slowest_models
        FROM `{self._table}`
        WHERE elapsed_time > @threshold_sec
          AND execution_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
        ORDER BY elapsed_time DESC
        LIMIT 100
        """
        return self._run(sql, params)

    def get_model_performance(self, *, lookback_hours: int = 24, top_n: int = 20) -> List[Dict[str, Any]]:
        """Top-N slowest dbt models by avg execution time."""
        params = [
            bigquery.ScalarQueryParameter("hours", "INT64", lookback_hours),
            bigquery.ScalarQueryParameter("top_n", "INT64", top_n),
        ]
        sql = f"""
        SELECT
            r.unique_id AS model_id,
            COUNT(*) AS executions,
            ROUND(AVG(r.execution_time), 2) AS avg_seconds,
            ROUND(MAX(r.execution_time), 2) AS max_seconds,
            ROUND(MIN(r.execution_time), 2) AS min_seconds,
            COUNTIF(r.status = 'error' OR r.failures > 0) AS failure_count,
            ROUND(AVG(r.adapter_response.bytes_billed) / POW(1024, 3), 4) AS avg_gb_billed,
            ROUND(AVG(r.adapter_response.slot_ms) / 1000, 2) AS avg_slot_seconds
        FROM `{self._table}`,
             UNNEST(results) AS r
        WHERE execution_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
        GROUP BY model_id
        ORDER BY avg_seconds DESC
        LIMIT @top_n
        """
        return self._run(sql, params)

    # ------------------------------------------------------------------
    # 3. Scheduling — execution lag analysis
    # ------------------------------------------------------------------

    def get_scheduling_lag(
        self,
        *,
        lookback_hours: int = 24,
        lag_threshold_minutes: int = 10,
    ) -> Dict[str, Any]:
        """Infer scheduling lag from execution_date vs first model start time."""
        params = [
            bigquery.ScalarQueryParameter("hours", "INT64", lookback_hours),
            bigquery.ScalarQueryParameter("threshold", "FLOAT64", lag_threshold_minutes),
        ]
        sql = f"""
        WITH lag_data AS (
            SELECT
                dag_id,
                task_id,
                run_id,
                execution_date,
                (SELECT MIN(t.started_at)
                 FROM UNNEST(results) r, UNNEST(r.timing) t
                 WHERE t.name = 'execute') AS first_model_start,
                TIMESTAMP_DIFF(
                    (SELECT MIN(t.started_at)
                     FROM UNNEST(results) r, UNNEST(r.timing) t
                     WHERE t.name = 'execute'),
                    execution_date,
                    SECOND
                ) / 60.0 AS lag_minutes
            FROM `{self._table}`
            WHERE execution_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
        )
        SELECT
            ROUND(APPROX_QUANTILES(lag_minutes, 100)[OFFSET(50)], 2) AS p50_lag_minutes,
            ROUND(APPROX_QUANTILES(lag_minutes, 100)[OFFSET(90)], 2) AS p90_lag_minutes,
            ROUND(APPROX_QUANTILES(lag_minutes, 100)[OFFSET(95)], 2) AS p95_lag_minutes,
            ROUND(AVG(lag_minutes), 2)  AS avg_lag_minutes,
            ROUND(MAX(lag_minutes), 2)  AS max_lag_minutes,
            COUNT(*)                    AS total_measurements
        FROM lag_data
        WHERE lag_minutes IS NOT NULL
        """
        rows = self._run(sql, params)
        stats = rows[0] if rows else {}

        # Delayed DAGs
        delayed_sql = f"""
        WITH lag_data AS (
            SELECT
                dag_id,
                task_id,
                run_id,
                execution_date,
                TIMESTAMP_DIFF(
                    (SELECT MIN(t.started_at)
                     FROM UNNEST(results) r, UNNEST(r.timing) t
                     WHERE t.name = 'execute'),
                    execution_date,
                    SECOND
                ) / 60.0 AS lag_minutes
            FROM `{self._table}`
            WHERE execution_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
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
    # 4. Dependencies — inferred task graph from timing
    # ------------------------------------------------------------------

    def get_inferred_task_graph(self, dag_id: str, run_id: str) -> Dict[str, Any]:
        """Reconstruct an approximate task dependency graph for a single DAG run
        by analysing temporal ordering of tasks."""
        params = [
            bigquery.ScalarQueryParameter("dag_id", "STRING", dag_id),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
        sql = f"""
        SELECT
            task_id,
            elapsed_time,
            ARRAY_LENGTH(results) AS num_models,
            (SELECT MIN(t.started_at) FROM UNNEST(results) r, UNNEST(r.timing) t WHERE t.name = 'execute') AS task_start,
            (SELECT MAX(t.completed_at) FROM UNNEST(results) r, UNNEST(r.timing) t WHERE t.name = 'execute') AS task_end,
            (SELECT ARRAY_AGG(r.unique_id ORDER BY r.unique_id) FROM UNNEST(results) r) AS model_ids
        FROM `{self._table}`
        WHERE dag_id = @dag_id AND run_id = @run_id
        ORDER BY task_start
        """
        tasks = self._run(sql, params)

        # Infer edges: task B depends on task A if A finished before B started
        # and there's no intermediate task C that also explains the dependency.
        edges: list[dict[str, str]] = []
        for i, task_b in enumerate(tasks):
            if task_b.get("task_start") is None:
                continue
            candidates = []
            for task_a in tasks[:i]:
                if task_a.get("task_end") is None:
                    continue
                if task_a["task_end"] <= task_b["task_start"]:
                    candidates.append(task_a)
            # Keep only the "closest" predecessors (remove transitive)
            direct = []
            for c in candidates:
                is_transitive = False
                for other in candidates:
                    if other["task_id"] != c["task_id"] and c["task_end"] <= other["task_start"]:
                        is_transitive = True
                        break
                if not is_transitive:
                    direct.append(c)
            for parent in direct:
                edges.append({"from": parent["task_id"], "to": task_b["task_id"]})

        # Serialise — convert datetime / list objects
        serialised_tasks = []
        for t in tasks:
            serialised_tasks.append({
                "task_id": t["task_id"],
                "elapsed_time": t.get("elapsed_time"),
                "num_models": t.get("num_models"),
                "task_start": t["task_start"].isoformat() if t.get("task_start") else None,
                "task_end": t["task_end"].isoformat() if t.get("task_end") else None,
                "model_ids": t.get("model_ids", []),
            })

        return {
            "dag_id": dag_id,
            "run_id": run_id,
            "tasks": serialised_tasks,
            "edges": edges,
            "task_count": len(tasks),
            "edge_count": len(edges),
        }

    # ------------------------------------------------------------------
    # 5. Overview / DAG list
    # ------------------------------------------------------------------

    def get_dag_overview(self, *, lookback_hours: int = 24) -> List[Dict[str, Any]]:
        """Per-DAG summary stats over the lookback window."""
        params = [bigquery.ScalarQueryParameter("hours", "INT64", lookback_hours)]
        sql = f"""
        SELECT
            dag_id,
            COUNT(DISTINCT run_id) AS total_runs,
            COUNT(DISTINCT task_id) AS unique_tasks,
            SUM(ARRAY_LENGTH(results)) AS total_dbt_models,
            SUM((SELECT COUNTIF(r.status = 'error' OR r.failures > 0) FROM UNNEST(results) r)) AS failed_models,
            ROUND(AVG(elapsed_time), 2) AS avg_elapsed_seconds,
            MAX(execution_date) AS latest_execution,
            ROUND(SUM((SELECT SUM(r.adapter_response.bytes_billed) FROM UNNEST(results) r)) / POW(1024, 3), 4) AS total_gb_billed
        FROM `{self._table}`
        WHERE execution_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
        GROUP BY dag_id
        ORDER BY total_runs DESC
        """
        return self._run(sql, params)

    # ------------------------------------------------------------------
    # 6. dbt-specific: cost analysis
    # ------------------------------------------------------------------

    def get_cost_analysis(self, *, lookback_hours: int = 24) -> Dict[str, Any]:
        """BigQuery cost analysis from adapter_response fields."""
        params = [bigquery.ScalarQueryParameter("hours", "INT64", lookback_hours)]
        sql = f"""
        WITH model_costs AS (
            SELECT
                dag_id,
                r.unique_id AS model_id,
                r.adapter_response.bytes_billed,
                r.adapter_response.slot_ms,
                r.execution_time
            FROM `{self._table}`,
                 UNNEST(results) AS r
            WHERE execution_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
              AND r.adapter_response.bytes_billed IS NOT NULL
        )
        SELECT
            COUNT(*) AS total_model_runs,
            ROUND(SUM(bytes_billed) / POW(1024, 3), 4) AS total_gb_billed,
            ROUND(SUM(slot_ms) / 1000 / 3600, 2) AS total_slot_hours,
            ROUND(AVG(bytes_billed) / POW(1024, 3), 6) AS avg_gb_per_model,
            ROUND(AVG(execution_time), 2) AS avg_execution_seconds
        FROM model_costs
        """
        rows = self._run(sql, params)
        summary = rows[0] if rows else {}

        # Top cost offenders
        top_sql = f"""
        SELECT
            r.unique_id AS model_id,
            dag_id,
            COUNT(*) AS executions,
            ROUND(SUM(r.adapter_response.bytes_billed) / POW(1024, 3), 4) AS total_gb_billed,
            ROUND(SUM(r.adapter_response.slot_ms) / 1000, 2) AS total_slot_seconds,
            ROUND(AVG(r.execution_time), 2) AS avg_seconds
        FROM `{self._table}`,
             UNNEST(results) AS r
        WHERE execution_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
          AND r.adapter_response.bytes_billed IS NOT NULL
        GROUP BY model_id, dag_id
        ORDER BY total_gb_billed DESC
        LIMIT 15
        """
        summary["top_cost_models"] = self._run(top_sql, params)
        return summary

    # ------------------------------------------------------------------
    # 7. Failure correlations
    # ------------------------------------------------------------------

    def get_failure_correlations(self, *, lookback_hours: int = 48) -> Dict[str, Any]:
        """Find DAGs that tend to fail around the same time."""
        params = [bigquery.ScalarQueryParameter("hours", "INT64", lookback_hours)]
        sql = f"""
        WITH failed_runs AS (
            SELECT DISTINCT
                dag_id,
                run_id,
                execution_date,
                DATE(execution_date) AS fail_date,
                EXTRACT(HOUR FROM execution_date) AS fail_hour
            FROM `{self._table}`,
                 UNNEST(results) AS r
            WHERE execution_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
              AND (r.status = 'error' OR r.failures > 0)
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
        SELECT
            dag_a,
            dag_b,
            COUNT(*) AS co_occurrences
        FROM pairs
        GROUP BY dag_a, dag_b
        HAVING co_occurrences >= 2
        ORDER BY co_occurrences DESC
        LIMIT 20
        """
        correlations = self._run(sql, params)
        return {
            "correlations": correlations,
            "lookback_hours": lookback_hours,
        }
