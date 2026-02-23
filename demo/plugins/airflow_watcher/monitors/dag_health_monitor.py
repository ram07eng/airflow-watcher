"""DAG Health Monitor - Tracks DAG parse errors and import issues."""

import logging
from datetime import timedelta
from typing import Dict, List, Optional

from airflow.models import DagModel, DagRun
from airflow.models import ImportError as AirflowImportError
from airflow.utils import timezone
from airflow.utils.session import provide_session
from sqlalchemy import func
from sqlalchemy.orm import Session

from airflow_watcher.config import WatcherConfig
from airflow_watcher.utils.cache import MetricsCache

logger = logging.getLogger(__name__)

# Cache TTLs (seconds)
SUMMARY_TTL = 60
COMPLEXITY_TTL = 300  # 5 min — expensive, rarely changes
INACTIVE_TTL = 120


class DAGHealthMonitor:
    """Monitor for tracking DAG-level health issues."""

    def __init__(self, config: Optional[WatcherConfig] = None):
        """Initialize the DAG health monitor."""
        self.config = config or WatcherConfig()
        self._cache = MetricsCache.get_instance()

    @provide_session
    def get_dag_import_errors(self, session: Session = None) -> List[Dict]:
        """Get DAGs with import/parse errors."""
        errors = session.query(AirflowImportError).all()

        import_errors = []
        for error in errors:
            import_errors.append(
                {
                    "filename": error.filename,
                    "timestamp": error.timestamp.isoformat() if error.timestamp else None,
                    "stacktrace": error.stacktrace[:500] if error.stacktrace else None,
                    "full_stacktrace": error.stacktrace,
                }
            )

        return import_errors

    @provide_session
    def get_dag_status_summary(self, session: Session = None) -> Dict:
        """Get overall DAG status summary with caching."""

        def _compute():
            return self._compute_dag_status_summary(session)

        return self._cache.get_or_compute("dag_status_summary", _compute, ttl=SUMMARY_TTL)

    def _compute_dag_status_summary(self, session: Session) -> Dict:
        """Compute DAG status summary — single pass with aggregated queries."""
        cutoff = timezone.utcnow() - timedelta(hours=24)

        # Single query: get all counts in one pass
        total_dags = session.query(DagModel).count()
        active_dags = session.query(DagModel).filter(DagModel.is_active).count()
        paused_dags = session.query(DagModel).filter(DagModel.is_paused).count()
        import_error_count = session.query(AirflowImportError).count()
        running_runs = session.query(DagRun).filter(DagRun.state == "running").count()
        recent_failures = session.query(DagRun).filter(DagRun.state == "failed", DagRun.end_date >= cutoff).count()

        return {
            "total_dags": total_dags,
            "active_dags": active_dags,
            "paused_dags": paused_dags,
            "dags_with_errors": import_error_count,
            "currently_running": running_runs,
            "recent_failures_24h": recent_failures,
            "health_score": self._calculate_health_score(total_dags, import_error_count, recent_failures),
            "timestamp": timezone.utcnow().isoformat(),
        }

    def _calculate_health_score(
        self,
        total_dags: int,
        error_count: int,
        failure_count: int,
        weights: Optional[dict] = None,
    ) -> int:
        """Calculate overall health score (0-100).

        Uses a weighted scoring model. Weights can be customised via the
        ``weights`` argument (keys: ``error_weight``, ``failure_weight``,
        ``error_cap``, ``failure_cap``).
        """
        if total_dags == 0:
            return 100

        defaults = {
            "error_weight": 15,
            "error_cap": 45,
            "failure_weight": 1,
            "failure_cap": 40,
        }
        w = {**defaults, **(weights or {})}

        score = 100
        error_ratio = error_count / total_dags
        error_deduction = min(error_ratio * w["error_weight"] * 100, w["error_cap"])
        score -= error_deduction

        failure_ratio = failure_count / total_dags
        failure_deduction = min(failure_ratio * w["failure_weight"] * 100, w["failure_cap"])
        score -= failure_deduction

        return max(0, round(score))

    @provide_session
    def get_dag_complexity_analysis(self, limit: int = 100, session: Session = None) -> List[Dict]:
        """Analyze DAG complexity metrics with caching.

        Uses DB queries instead of DagBag to avoid loading all DAGs into memory.
        At 4500+ DAGs, DagBag deserialization can take 30+ seconds.
        """

        def _compute():
            return self._compute_complexity(limit, session)

        return self._cache.get_or_compute(f"dag_complexity_{limit}", _compute, ttl=COMPLEXITY_TTL)

    def _compute_complexity(self, limit: int, session: Session) -> List[Dict]:
        """Compute complexity using DB queries — avoids DagBag."""
        from airflow.models.serialized_dag import SerializedDagModel

        try:
            # Query serialized DAGs for task counts without loading DagBag
            serialized_dags = session.query(SerializedDagModel.dag_id, SerializedDagModel.data).limit(limit).all()
        except Exception as e:
            logger.warning(f"SerializedDagModel query failed, falling back to DagBag: {e}")
            return self._compute_complexity_dagbag(limit)

        complexity_data = []
        for dag_id, data in serialized_dags:
            try:
                dag_dict = data if isinstance(data, dict) else {}
                dag_data = dag_dict.get("dag", dag_dict)
                tasks = dag_data.get("tasks", [])
                task_count = len(tasks)

                # Count dependencies from serialized data
                dep_count = 0
                for task in tasks:
                    dep_count += len(task.get("upstream_task_ids", []))

                complexity_data.append(
                    {
                        "dag_id": dag_id,
                        "task_count": task_count,
                        "dependency_count": dep_count,
                        "avg_dependencies_per_task": round(dep_count / task_count, 2) if task_count > 0 else 0,
                    }
                )
            except Exception:
                continue

        return sorted(complexity_data, key=lambda x: x["task_count"], reverse=True)

    def _compute_complexity_dagbag(self, limit: int) -> List[Dict]:
        """Fallback: compute complexity via DagBag (slow at scale)."""
        from airflow.models import DagBag

        try:
            dagbag = DagBag(read_dags_from_db=True)
        except Exception as e:
            logger.error(f"Failed to load DagBag: {e}")
            return []

        complexity_data = []
        for i, (dag_id, dag) in enumerate(dagbag.dags.items()):
            if i >= limit:
                break
            task_count = len(dag.tasks)
            dependency_count = sum(len(task.upstream_list) for task in dag.tasks)
            max_depth = self._calculate_dag_depth(dag)

            complexity_data.append(
                {
                    "dag_id": dag_id,
                    "task_count": task_count,
                    "dependency_count": dependency_count,
                    "max_depth": max_depth,
                    "avg_dependencies_per_task": round(dependency_count / task_count, 2) if task_count > 0 else 0,
                    "schedule_interval": str(dag.schedule_interval),
                    "is_paused": dag.is_paused,
                    "tags": [t.name for t in dag.tags] if dag.tags else [],
                }
            )

        return sorted(complexity_data, key=lambda x: x["task_count"], reverse=True)

    def _calculate_dag_depth(self, dag) -> int:
        """Calculate the maximum depth of a DAG."""
        if not dag.tasks:
            return 0

        roots = [t for t in dag.tasks if not t.upstream_list]
        if not roots:
            return 0

        def get_depth(task, visited=None):
            if visited is None:
                visited = set()
            if task.task_id in visited:
                return 0
            visited.add(task.task_id)
            if not task.downstream_list:
                return 1
            return 1 + max(get_depth(downstream, visited.copy()) for downstream in task.downstream_list)

        return max(get_depth(root) for root in roots)

    @provide_session
    def get_inactive_dags(
        self,
        inactive_days: int = 30,
        limit: int = 200,
        session: Session = None,
    ) -> List[Dict]:
        """Find DAGs that haven't run recently.

        Uses a single JOIN query instead of N+1 queries.
        At 4500 DAGs, the old code ran 4500 individual queries.
        """

        def _compute():
            return self._compute_inactive_dags(inactive_days, limit, session)

        return self._cache.get_or_compute(f"inactive_dags_{inactive_days}", _compute, ttl=INACTIVE_TTL)

    def _compute_inactive_dags(self, inactive_days: int, limit: int, session: Session) -> List[Dict]:
        """Single query: LEFT JOIN DagModel with latest DagRun."""
        cutoff = timezone.utcnow() - timedelta(days=inactive_days)

        # Subquery: latest execution_date per dag_id
        latest_run_sq = (
            session.query(
                DagRun.dag_id,
                func.max(DagRun.execution_date).label("latest_execution"),
            )
            .group_by(DagRun.dag_id)
            .subquery()
        )

        # Single JOIN query — replaces N+1
        results = (
            session.query(
                DagModel.dag_id,
                DagModel.is_paused,
                DagModel.schedule_interval,
                latest_run_sq.c.latest_execution,
            )
            .outerjoin(latest_run_sq, DagModel.dag_id == latest_run_sq.c.dag_id)
            .filter(
                DagModel.is_active.is_(True),
                (latest_run_sq.c.latest_execution < cutoff) | (latest_run_sq.c.latest_execution.is_(None)),
            )
            .limit(limit)
            .all()
        )

        inactive_dags = []
        now = timezone.utcnow()
        for dag_id, is_paused, schedule_interval, latest_execution in results:
            days_inactive = None
            if latest_execution:
                days_inactive = (now - latest_execution).days

            inactive_dags.append(
                {
                    "dag_id": dag_id,
                    "last_run": latest_execution.isoformat() if latest_execution else None,
                    "days_inactive": days_inactive,
                    "is_paused": is_paused,
                    "schedule_interval": str(schedule_interval) if schedule_interval else None,
                    "recommendation": "Consider pausing or removing"
                    if days_inactive and days_inactive > 60
                    else "Review",
                }
            )

        return sorted(inactive_dags, key=lambda x: x["days_inactive"] or 999, reverse=True)
