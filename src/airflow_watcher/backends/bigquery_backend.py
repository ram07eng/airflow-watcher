"""ExternalBackend — monitor interface adapter for BQClient / SQLClient.

A single instance satisfies every monitor interface (failure, SLA, task,
scheduling, dag-health, dependency).  ``monitor_provider.py`` returns it
instead of the Airflow monitors when
``AIRFLOW_WATCHER_BACKEND=bigquery|sqlalchemy``.

The underlying client (``BQClient`` or ``SQLClient``) is injected at
construction time; this class only adapts the monitor API to client
method calls.

Methods not supportable by external backends (SLA, retries, zombies,
pools, live queue, import errors, cross-dag graph, cascading impact)
return safe empty values with a ``backend_note`` key.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, cast

logger = logging.getLogger(__name__)


class _Row(dict):
    """Dict subclass with a no-op ``.to_dict()`` method.

    The failures and SLA routers call ``f.to_dict()`` on each result row.
    Returning ``self`` keeps the router code unchanged while external
    backends already yield plain dicts.
    """

    def to_dict(self) -> "_Row":
        return self


# Keep the old name as an alias so existing imports/tests don't break.
BigQueryBackend = None  # set at bottom of file after class definition


class ExternalBackend:
    """Unified backend adapter — works with any client that satisfies the
    BQClient / SQLClient interface.

    Args:
        client: An instance of ``BQClient`` or ``SQLClient``.
        backend_label: Human-readable label for log messages and
                       ``backend_note`` fields (e.g. ``"bigquery"``).
    """

    def __init__(self, client: Any, backend_label: str = "external") -> None:
        self._client = client
        self._label = backend_label
        logger.info("ExternalBackend initialised (backend=%s)", backend_label)

    # ==================================================================
    # Failure monitor interface
    # ==================================================================

    def get_recent_failures(
        self,
        *,
        dag_id: Optional[str] = None,
        lookback_hours: int = 24,
        limit: int = 50,
        offset: int = 0,
    ) -> List[_Row]:
        rows = self._client.get_recent_failures(
            dag_id=dag_id,
            lookback_hours=lookback_hours,
            limit=limit,
            offset=offset,
        )
        return [_Row(r) for r in rows]

    def get_failure_statistics(self, *, lookback_hours: int = 24) -> Dict[str, Any]:
        return cast(Dict[str, Any], self._client.get_failure_statistics(lookback_hours=lookback_hours))

    # ==================================================================
    # SLA monitor interface
    # ==================================================================

    def get_recent_sla_misses(
        self,
        *,
        dag_id: Optional[str] = None,
        lookback_hours: int = 24,
        limit: int = 50,
        offset: int = 0,
    ) -> List[_Row]:
        return []

    def get_sla_statistics(self, *, lookback_hours: int = 24) -> Dict[str, Any]:
        return {
            "total_misses": 0,
            "miss_rate": 0.0,
            "top_dags_with_misses": [],
            "top_tasks_with_misses": [],
            "backend_note": f"SLA data is not available in the {self._label} backend.",
        }

    # ==================================================================
    # Task monitor interface
    # ==================================================================

    def get_long_running_tasks(self, *, threshold_minutes: int = 60) -> List[Dict[str, Any]]:
        return cast(List[Dict[str, Any]], self._client.get_long_running_tasks(threshold_minutes=threshold_minutes))

    def get_retry_heavy_tasks(self, *, lookback_hours: int = 24, min_retries: int = 2) -> List[Dict[str, Any]]:
        return []

    def get_zombie_tasks(self, *, zombie_threshold_minutes: int = 120) -> List[Dict[str, Any]]:
        return []

    def get_task_failure_patterns(self, *, lookback_hours: int = 168) -> Dict[str, Any]:
        stats = self._client.get_failure_statistics(lookback_hours=lookback_hours)
        top_failing = [
            {
                "dag_id": d.get("dag_id"),
                "task_id": None,
                "failure_count": d.get("failure_count"),
                "total_runs": d.get("total_runs"),
                "failure_rate": d.get("failure_rate"),
            }
            for d in stats.get("most_failing_dags", [])
        ]
        return {
            "top_failing_tasks": top_failing,
            "flaky_tasks": [],
            "consistently_failing_tasks": [t for t in top_failing if (t.get("failure_rate") or 0) >= 0.8],
            "lookback_hours": lookback_hours,
            "backend_note": f"Pattern analysis based on data from the {self._label} backend.",
        }

    # ==================================================================
    # Scheduling monitor interface
    # ==================================================================

    def get_scheduling_lag(
        self,
        *,
        lookback_hours: int = 24,
        lag_threshold_minutes: int = 10,
    ) -> Dict[str, Any]:
        return cast(
            Dict[str, Any],
            self._client.get_scheduling_lag(
                lookback_hours=lookback_hours,
                lag_threshold_minutes=lag_threshold_minutes,
            ),
        )

    def get_queued_tasks(self) -> Dict[str, Any]:
        return {
            "queued_count": 0,
            "scheduled_count": 0,
            "queued_tasks": [],
            "scheduled_tasks": [],
            "backend_note": f"Live queue data is not available in the {self._label} backend.",
        }

    def get_pool_utilization(self) -> List[Dict[str, Any]]:
        return []

    def get_stale_dags(self, *, expected_interval_hours: int = 24) -> List[Dict[str, Any]]:
        from datetime import datetime, timezone

        overview = self._client.get_dag_overview(lookback_hours=expected_interval_hours)
        stale = []
        for row in overview:
            latest = row.get("latest_execution")
            if latest is None:
                continue
            if hasattr(latest, "tzinfo") and latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)
            age_hours = (datetime.now(tz=timezone.utc) - latest).total_seconds() / 3600
            if age_hours > expected_interval_hours:
                stale.append(
                    {
                        "dag_id": row.get("dag_id"),
                        "latest_execution": (latest.isoformat() if hasattr(latest, "isoformat") else str(latest)),
                        "hours_since_last_run": round(age_hours, 2),
                    }
                )
        return stale

    def get_concurrent_runs(self) -> Dict[str, Any]:
        return {
            "dags_with_concurrent_runs": 0,
            "concurrent_dags": [],
            "backend_note": f"Concurrent run data is not available in the {self._label} backend.",
        }

    # ==================================================================
    # DAG health monitor interface
    # ==================================================================

    def get_dag_import_errors(self) -> List[Dict[str, Any]]:
        return []

    def get_dag_status_summary(self) -> Dict[str, Any]:
        overview = self._client.get_dag_overview(lookback_hours=24)
        total = len(overview)
        with_failures = sum(1 for d in overview if (d.get("failed_models") or 0) > 0)
        healthy = total - with_failures
        score = round((healthy / total * 100) if total else 100, 1)
        return {
            "total_dags": total,
            "active_dags": total,
            "paused_dags": 0,
            "dags_with_failures": with_failures,
            "health_score": score,
            "backend_note": f"Summary derived from {self._label} data (last 24 h).",
        }

    def get_dag_complexity_analysis(self) -> List[Dict[str, Any]]:
        overview = self._client.get_dag_overview(lookback_hours=168)
        return [
            {
                "dag_id": row.get("dag_id"),
                "task_count": row.get("unique_tasks", 0),
                "total_runs": row.get("total_runs", 0),
                "backend_note": "task_count = unique task IDs seen in the last 7 days.",
            }
            for row in overview
        ]

    def get_inactive_dags(self, *, inactive_days: int = 30) -> List[Dict[str, Any]]:
        return self.get_stale_dags(expected_interval_hours=inactive_days * 24)

    def get_dag_health_status(self, limit: int = 500) -> Dict[str, Any]:
        overview = self._client.get_dag_overview(lookback_hours=24)
        healthy = [r.get("dag_id") for r in overview if (r.get("failed_models") or 0) == 0]
        unhealthy = [r.get("dag_id") for r in overview if (r.get("failed_models") or 0) > 0]
        return {
            "healthy": healthy,
            "unhealthy": unhealthy,
            "running": [],
            "unknown": [],
            "summary": {"healthy": len(healthy), "unhealthy": len(unhealthy)},
            "backend_note": f"Health derived from {self._label} data (last 24 h).",
        }

    # ==================================================================
    # Dependency monitor interface
    # ==================================================================

    def get_upstream_failures(self, *, lookback_hours: int = 24) -> List[Dict[str, Any]]:
        rows = self._client.get_recent_failures(lookback_hours=lookback_hours, limit=200)
        return [
            {
                "dag_id": r.get("dag_id"),
                "task_id": r.get("task_id"),
                "run_id": r.get("run_id"),
                "execution_date": (
                    r["execution_date"].isoformat()
                    if hasattr(r.get("execution_date"), "isoformat")
                    else r.get("execution_date")
                ),
                "model_id": r.get("model_unique_id"),
                "state": "error",
                "backend_note": (f"upstream_failed state not in {self._label}; showing model errors."),
            }
            for r in rows
        ]

    def get_cross_dag_dependencies(self) -> List[Dict[str, Any]]:
        return []

    def get_failure_correlation(self, *, lookback_hours: int = 24) -> Dict[str, Any]:
        return cast(Dict[str, Any], self._client.get_failure_correlations(lookback_hours=lookback_hours))

    def get_cascading_failure_impact(self, *, dag_id: str, task_id: str) -> Dict[str, Any]:
        return {
            "dag_id": dag_id,
            "task_id": task_id,
            "downstream": [],
            "impacted_tasks": [],
            "backend_note": "Cascading impact analysis requires a full Airflow install.",
        }


# Backwards-compatible alias — existing code that imports BigQueryBackend still works.
BigQueryBackend = ExternalBackend
