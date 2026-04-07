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

logger = logging.getLogger(__name__)


class DAGHealthMonitor:
    """Monitor for tracking DAG-level health issues."""

    def __init__(self, config: Optional[WatcherConfig] = None):
        """Initialize the DAG health monitor."""
        self.config = config or WatcherConfig()

    @provide_session
    def get_dag_import_errors(
        self,
        session: Session = None,  # type: ignore[assignment]
    ) -> List[Dict]:
        """Get DAGs with import/parse errors.

        Args:
            session: SQLAlchemy session

        Returns:
            List of DAGs with import errors
        """
        errors = session.query(AirflowImportError).all()

        import_errors = []
        for error in errors:
            import_errors.append(
                {
                    "filename": error.filename,
                    "timestamp": error.timestamp.isoformat() if error.timestamp else None,
                    "stacktrace": error.stacktrace[:500] if error.stacktrace else None,  # Truncate
                    "full_stacktrace": error.stacktrace,
                }
            )

        return import_errors

    @provide_session
    def get_dag_status_summary(
        self,
        session: Session = None,  # type: ignore[assignment]
    ) -> Dict:
        """Get overall DAG status summary.

        Args:
            session: SQLAlchemy session

        Returns:
            Dictionary with DAG status summary
        """
        # Total DAGs
        total_dags = session.query(DagModel).count()

        # Active DAGs
        active_dags = (
            session.query(DagModel)
            .filter(
                DagModel.is_active,
            )
            .count()
        )

        # Paused DAGs
        paused_dags = (
            session.query(DagModel)
            .filter(
                DagModel.is_paused,
            )
            .count()
        )

        # DAGs with import errors
        import_error_count = session.query(AirflowImportError).count()

        # Running DAG runs
        running_runs = (
            session.query(DagRun)
            .filter(
                DagRun.state == "running",
            )
            .count()
        )

        # Recent failures (24h)
        cutoff = timezone.utcnow() - timedelta(hours=24)
        recent_failures = (
            session.query(DagRun)
            .filter(
                DagRun.state == "failed",
                DagRun.end_date >= cutoff,
            )
            .count()
        )

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
        weights: dict = None,  # type: ignore[assignment]
    ) -> int:
        """Calculate overall health score (0-100).

        Uses a weighted scoring model. Weights can be customised via the
        ``weights`` argument (keys: ``error_weight``, ``failure_weight``,
        ``error_cap``, ``failure_cap``).

        Args:
            total_dags: Total number of DAGs
            error_count: Number of DAGs with import/parse errors
            failure_count: Number of recent failures in the last 24 h
            weights: Optional dict to override default scoring weights

        Returns:
            Health score from 0-100
        """
        if total_dags == 0:
            return 100

        defaults = {
            "error_weight": 15,  # points deducted per import error
            "error_cap": 45,  # max deduction from errors
            "failure_weight": 1,  # points deducted per failure
            "failure_cap": 40,  # max deduction from failures
        }
        w = {**defaults, **(weights or {})}

        score = 100

        # Proportional error penalty (scales with DAG count)
        error_ratio = error_count / total_dags
        error_deduction = min(error_ratio * w["error_weight"] * 100, w["error_cap"])
        score -= error_deduction

        # Proportional failure penalty
        failure_ratio = failure_count / total_dags
        failure_deduction = min(failure_ratio * w["failure_weight"] * 100, w["failure_cap"])
        score -= failure_deduction

        return int(max(0, round(score)))

    @provide_session
    def get_dag_complexity_analysis(
        self,
        limit: int = 100,
        session: Session = None,  # type: ignore[assignment]
    ) -> List[Dict]:
        """Analyze DAG complexity metrics.

        Args:
            limit: Maximum number of DAGs to analyse (avoids loading all DAGs)
            session: SQLAlchemy session

        Returns:
            List of DAGs with complexity metrics
        """
        from airflow.models import DagBag

        # Query active DAG IDs from metadata (lightweight) instead of
        # loading every DAG object through DagBag eagerly.
        dag_ids = [row.dag_id for row in session.query(DagModel.dag_id).filter(DagModel.is_active).limit(limit).all()]

        try:
            dagbag = DagBag(include_examples=False, read_dags_from_db=True)
        except Exception as e:
            logger.error(f"Failed to load DagBag: {e}")
            return []

        complexity_data = []

        for dag_id in dag_ids:
            try:
                dag = dagbag.get_dag(dag_id)
            except Exception:
                continue
            if not dag:
                continue

            task_count = len(dag.tasks)

            # Count dependencies
            dependency_count = sum(len(task.upstream_list) for task in dag.tasks)

            # Calculate depth (longest path)
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
                    "tags": [t.name if hasattr(t, "name") else str(t) for t in dag.tags] if dag.tags else [],
                }
            )

        return sorted(complexity_data, key=lambda x: x["task_count"], reverse=True)

    def _calculate_dag_depth(self, dag) -> int:
        """Calculate the maximum depth of a DAG using memoized DFS."""
        if not dag.tasks:
            return 0

        # Find root tasks (no upstream)
        roots = [t for t in dag.tasks if not t.upstream_list]

        if not roots:
            return 0

        memo: dict[str, int] = {}  # task_id -> depth
        visiting: set = set()  # cycle detection

        def get_depth(task) -> int:
            if task.task_id in memo:
                return memo[task.task_id]
            if task.task_id in visiting:
                return 0  # Cycle detected

            visiting.add(task.task_id)

            if not task.downstream_list:
                depth = 1
            else:
                depth = 1 + max(get_depth(downstream) for downstream in task.downstream_list)

            visiting.discard(task.task_id)
            memo[task.task_id] = depth
            return depth

        return int(max(get_depth(root) for root in roots))

    @provide_session
    def get_inactive_dags(
        self,
        inactive_days: int = 30,
        session: Session = None,  # type: ignore[assignment]
    ) -> List[Dict]:
        """Find DAGs that haven't been modified or run recently.

        Args:
            inactive_days: Days of inactivity threshold
            session: SQLAlchemy session

        Returns:
            List of inactive DAGs
        """
        cutoff = timezone.utcnow() - timedelta(days=inactive_days)
        current_time = timezone.utcnow()

        # Get all active DAGs
        all_dags = (
            session.query(DagModel)
            .filter(
                DagModel.is_active,
            )
            .all()
        )

        if not all_dags:
            return []

        dag_ids = [d.dag_id for d in all_dags]
        dag_map = {d.dag_id: d for d in all_dags}

        # Batch query: get most recent execution_date per DAG in one query
        latest_runs = (
            session.query(
                DagRun.dag_id,
                func.max(DagRun.execution_date).label("latest_execution"),
            )
            .filter(DagRun.dag_id.in_(dag_ids))
            .group_by(DagRun.dag_id)
            .all()
        )
        latest_by_dag = {row.dag_id: row.latest_execution for row in latest_runs}

        inactive_dags = []
        for dag_id in dag_ids:
            last_run_date = latest_by_dag.get(dag_id)
            dag = dag_map[dag_id]

            if not last_run_date or last_run_date < cutoff:
                days_inactive = None
                if last_run_date:
                    days_inactive = (current_time - last_run_date).days

                inactive_dags.append(
                    {
                        "dag_id": dag_id,
                        "last_run": last_run_date.isoformat() if last_run_date else None,
                        "days_inactive": days_inactive,
                        "is_paused": dag.is_paused,
                        "schedule_interval": str(dag.schedule_interval) if dag.schedule_interval else None,
                        "recommendation": "Consider pausing or removing"
                        if days_inactive and days_inactive > 60
                        else "Review",
                    }
                )

        return sorted(inactive_dags, key=lambda x: x["days_inactive"] or 999, reverse=True)
