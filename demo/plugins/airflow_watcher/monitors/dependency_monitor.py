"""Dependency Monitor - Tracks upstream/downstream failures and cascading issues."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

from airflow.models import DagRun, TaskInstance
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState, TaskInstanceState
from sqlalchemy.orm import Session

from airflow_watcher.config import WatcherConfig
from airflow_watcher.utils.cache import MetricsCache

logger = logging.getLogger(__name__)

UPSTREAM_FAILURES_TTL = 60
CROSS_DAG_TTL = 300  # 5 min — expensive
CORRELATION_TTL = 120


class DependencyMonitor:
    """Monitor for tracking dependency-related issues."""

    def __init__(self, config: Optional[WatcherConfig] = None):
        """Initialize the dependency monitor."""
        self.config = config or WatcherConfig()
        self._cache = MetricsCache.get_instance()

    @provide_session
    def get_upstream_failures(
        self,
        lookback_hours: int = 24,
        session: Session = None,
    ) -> List[Dict]:
        """Find tasks in upstream_failed state."""
        cutoff = timezone.utcnow() - timedelta(hours=lookback_hours)

        query = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.state == TaskInstanceState.UPSTREAM_FAILED,
                TaskInstance.end_date >= cutoff,
            )
            .order_by(TaskInstance.end_date.desc())
        )

        upstream_failures = []
        for ti in query.limit(100).all():
            upstream_failures.append(
                {
                    "dag_id": ti.dag_id,
                    "task_id": ti.task_id,
                    "run_id": ti.run_id,
                    "execution_date": ti.execution_date.isoformat() if ti.execution_date else None,
                    "timestamp": ti.end_date.isoformat() if ti.end_date else None,
                }
            )

        return upstream_failures

    @provide_session
    def get_cascading_failure_impact(
        self,
        dag_id: str,
        task_id: str,
        session: Session = None,
    ) -> Dict:
        """Analyze the downstream impact of a task failure."""
        from airflow.models import DagBag

        try:
            dagbag = DagBag(read_dags_from_db=True)
            dag = dagbag.get_dag(dag_id)
        except Exception as e:
            logger.error(f"Failed to load DAG {dag_id}: {e}")
            return {"error": str(e)}

        if not dag:
            return {"error": f"DAG {dag_id} not found"}

        task = dag.get_task(task_id)
        if not task:
            return {"error": f"Task {task_id} not found in DAG {dag_id}"}

        affected_tasks = self._get_all_downstream(task, set())

        return {
            "dag_id": dag_id,
            "failed_task": task_id,
            "affected_task_count": len(affected_tasks),
            "affected_tasks": list(affected_tasks),
            "impact_percentage": round(len(affected_tasks) / len(dag.tasks) * 100, 2) if dag.tasks else 0,
            "timestamp": timezone.utcnow().isoformat(),
        }

    def _get_all_downstream(self, task, visited: Set[str]) -> Set[str]:
        """Recursively get all downstream tasks."""
        downstream_ids = set()

        for downstream in task.downstream_list:
            if downstream.task_id not in visited:
                visited.add(downstream.task_id)
                downstream_ids.add(downstream.task_id)
                downstream_ids.update(self._get_all_downstream(downstream, visited))

        return downstream_ids

    @provide_session
    def get_cross_dag_dependencies(self, session: Session = None) -> List[Dict]:
        """Identify cross-DAG dependencies using serialized DAG data.

        Uses SerializedDagModel instead of DagBag to avoid loading all DAGs
        into memory. At 4500+ DAGs, DagBag takes 30+ seconds.
        """

        def _compute():
            return self._compute_cross_dag_deps(session)

        return self._cache.get_or_compute("cross_dag_deps", _compute, ttl=CROSS_DAG_TTL)

    def _compute_cross_dag_deps(self, session: Session) -> List[Dict]:
        """Query serialized DAGs for ExternalTaskSensor/TriggerDagRunOperator."""
        try:
            from airflow.models.serialized_dag import SerializedDagModel

            serialized_dags = session.query(SerializedDagModel.dag_id, SerializedDagModel.data).all()
        except Exception as e:
            logger.warning(f"SerializedDagModel query failed, falling back to DagBag: {e}")
            return self._compute_cross_dag_deps_dagbag()

        dependencies: List[Dict] = []

        for dag_id, data in serialized_dags:
            try:
                dag_dict = data if isinstance(data, dict) else {}
                dag_data = dag_dict.get("dag", dag_dict)
                tasks = dag_data.get("tasks", [])

                for task in tasks:
                    task_type = task.get("task_type", "")
                    task_id = task.get("task_id", "")

                    if task_type == "ExternalTaskSensor":
                        ext_dag = task.get("external_dag_id")
                        ext_task = task.get("external_task_id")
                        if ext_dag:
                            dependencies.append(
                                {
                                    "dag_id": dag_id,
                                    "task_id": task_id,
                                    "depends_on_dag": ext_dag,
                                    "depends_on_task": ext_task,
                                    "dependency_type": "ExternalTaskSensor",
                                }
                            )

                    if task_type in ("TriggerDagRunOperator", "TriggerDagRun"):
                        trigger_dag = task.get("trigger_dag_id")
                        if trigger_dag:
                            dependencies.append(
                                {
                                    "dag_id": dag_id,
                                    "task_id": task_id,
                                    "triggers_dag": trigger_dag,
                                    "dependency_type": "TriggerDagRunOperator",
                                }
                            )
            except Exception:
                continue

        return dependencies

    def _compute_cross_dag_deps_dagbag(self) -> List[Dict]:
        """Fallback: use DagBag for cross-DAG dependency detection."""
        from airflow.models import DagBag

        try:
            dagbag = DagBag(read_dags_from_db=True)
        except Exception as e:
            logger.error(f"Failed to load DagBag: {e}")
            return []

        dependencies: List[Dict] = []

        for dag_id, dag in dagbag.dags.items():
            for task in dag.tasks:
                if task.task_type == "ExternalTaskSensor":
                    try:
                        external_dag_id = getattr(task, "external_dag_id", None)
                        external_task_id = getattr(task, "external_task_id", None)
                        if external_dag_id:
                            dependencies.append(
                                {
                                    "dag_id": dag_id,
                                    "task_id": task.task_id,
                                    "depends_on_dag": external_dag_id,
                                    "depends_on_task": external_task_id,
                                    "dependency_type": "ExternalTaskSensor",
                                }
                            )
                    except Exception:
                        pass

                if task.task_type in ("TriggerDagRunOperator", "TriggerDagRun"):
                    try:
                        trigger_dag_id = getattr(task, "trigger_dag_id", None)
                        if trigger_dag_id:
                            dependencies.append(
                                {
                                    "dag_id": dag_id,
                                    "task_id": task.task_id,
                                    "triggers_dag": trigger_dag_id,
                                    "dependency_type": "TriggerDagRunOperator",
                                }
                            )
                    except Exception:
                        pass

        return dependencies

    @provide_session
    def get_dependency_chain_status(
        self,
        dag_id: str,
        execution_date: datetime,
        session: Session = None,
    ) -> Dict:
        """Get status of all tasks in a DAG run's dependency chain."""
        task_instances = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.execution_date == execution_date,
            )
            .all()
        )

        if not task_instances:
            return {"error": "No task instances found"}

        state_counts: Dict[str, int] = {}
        tasks_by_state: Dict[str, List[str]] = {}

        for ti in task_instances:
            state = str(ti.state)
            state_counts[state] = state_counts.get(state, 0) + 1

            if state not in tasks_by_state:
                tasks_by_state[state] = []
            tasks_by_state[state].append(ti.task_id)

        bottlenecks = []
        for ti in task_instances:
            if ti.state == TaskInstanceState.RUNNING:
                if state_counts.get(str(TaskInstanceState.SCHEDULED), 0) > 0:
                    bottlenecks.append(ti.task_id)

        return {
            "dag_id": dag_id,
            "execution_date": execution_date.isoformat(),
            "total_tasks": len(task_instances),
            "state_summary": state_counts,
            "tasks_by_state": tasks_by_state,
            "potential_bottlenecks": bottlenecks,
            "is_blocked": len(tasks_by_state.get(str(TaskInstanceState.UPSTREAM_FAILED), [])) > 0,
            "timestamp": timezone.utcnow().isoformat(),
        }

    @provide_session
    def get_failure_correlation(
        self,
        lookback_hours: int = 24,
        session: Session = None,
    ) -> Dict:
        """Find DAGs that often fail together.

        Optimized: uses bucket-based grouping instead of O(n^2) nested loop.
        At 200 failures, the old code did 40,000 comparisons.
        """

        def _compute():
            return self._compute_failure_correlation(lookback_hours, session)

        return self._cache.get_or_compute(f"failure_correlation_{lookback_hours}", _compute, ttl=CORRELATION_TTL)

    def _compute_failure_correlation(self, lookback_hours: int, session: Session) -> Dict:
        """Bucket-based O(n) correlation instead of O(n^2)."""
        cutoff = timezone.utcnow() - timedelta(hours=lookback_hours)
        window_minutes = 10

        # Limit to 500 most recent failures to bound computation
        failed_runs = (
            session.query(DagRun.dag_id, DagRun.end_date)
            .filter(
                DagRun.state == DagRunState.FAILED,
                DagRun.end_date >= cutoff,
            )
            .order_by(DagRun.end_date)
            .limit(500)
            .all()
        )

        if len(failed_runs) < 2:
            return {
                "period_hours": lookback_hours,
                "correlations": [],
                "message": "Not enough failures to analyze correlations",
            }

        # Bucket failures into time windows (O(n) instead of O(n^2))
        pair_counts: Dict[tuple, Dict] = {}

        # Group runs into buckets by time window
        buckets: Dict[int, List[str]] = defaultdict(list)
        for dag_id, end_date in failed_runs:
            bucket_key = int(end_date.timestamp() // (window_minutes * 60))
            buckets[bucket_key].append(dag_id)

        # Check adjacent buckets for co-failures
        sorted_keys = sorted(buckets.keys())
        for idx, key in enumerate(sorted_keys):
            # Combine current bucket with next bucket (handles boundary cases)
            combined_dags = list(buckets[key])
            if idx + 1 < len(sorted_keys) and sorted_keys[idx + 1] == key + 1:
                combined_dags.extend(buckets[sorted_keys[idx + 1]])

            # Find unique DAG pairs in this window
            unique_dags = list(set(combined_dags))
            for i in range(len(unique_dags)):
                for j in range(i + 1, len(unique_dags)):
                    pair = tuple(sorted([unique_dags[i], unique_dags[j]]))
                    if pair not in pair_counts:
                        pair_counts[pair] = {
                            "dag_1": pair[0],
                            "dag_2": pair[1],
                            "co_failure_count": 0,
                        }
                    pair_counts[pair]["co_failure_count"] += 1

        correlations: List[Dict] = sorted(pair_counts.values(), key=lambda x: x["co_failure_count"], reverse=True)[:20]

        return {
            "period_hours": lookback_hours,
            "window_minutes": window_minutes,
            "correlations": correlations,
            "timestamp": timezone.utcnow().isoformat(),
        }
