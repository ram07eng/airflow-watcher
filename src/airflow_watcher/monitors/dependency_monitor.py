"""Dependency Monitor - Tracks upstream/downstream failures and cascading issues."""

from datetime import datetime, timedelta
from typing import List, Optional, Dict, Set
import logging

from airflow.models import DagRun, TaskInstance, DagModel
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session
from sqlalchemy import func

from airflow_watcher.config import WatcherConfig


logger = logging.getLogger(__name__)


class DependencyMonitor:
    """Monitor for tracking dependency-related issues."""

    def __init__(self, config: Optional[WatcherConfig] = None):
        """Initialize the dependency monitor."""
        self.config = config or WatcherConfig()

    @provide_session
    def get_upstream_failures(
        self,
        lookback_hours: int = 24,
        session: Session = None,
    ) -> List[Dict]:
        """Find tasks in upstream_failed state.
        
        Args:
            lookback_hours: Hours to look back
            session: SQLAlchemy session
            
        Returns:
            List of upstream failed tasks
        """
        cutoff = datetime.utcnow() - timedelta(hours=lookback_hours)
        
        query = session.query(TaskInstance).filter(
            TaskInstance.state == TaskInstanceState.UPSTREAM_FAILED,
            TaskInstance.end_date >= cutoff,
        ).order_by(TaskInstance.end_date.desc())
        
        upstream_failures = []
        for ti in query.limit(100).all():
            upstream_failures.append({
                "dag_id": ti.dag_id,
                "task_id": ti.task_id,
                "run_id": ti.run_id,
                "execution_date": ti.execution_date.isoformat() if ti.execution_date else None,
                "timestamp": ti.end_date.isoformat() if ti.end_date else None,
            })
        
        return upstream_failures

    @provide_session
    def get_cascading_failure_impact(
        self,
        dag_id: str,
        task_id: str,
        session: Session = None,
    ) -> Dict:
        """Analyze the downstream impact of a task failure.
        
        Args:
            dag_id: DAG ID
            task_id: Failed task ID
            session: SQLAlchemy session
            
        Returns:
            Dictionary with impact analysis
        """
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
        
        # Get all downstream tasks recursively
        affected_tasks = self._get_all_downstream(task, set())
        
        return {
            "dag_id": dag_id,
            "failed_task": task_id,
            "affected_task_count": len(affected_tasks),
            "affected_tasks": list(affected_tasks),
            "impact_percentage": round(len(affected_tasks) / len(dag.tasks) * 100, 2) if dag.tasks else 0,
            "timestamp": datetime.utcnow().isoformat(),
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
        """Identify cross-DAG dependencies using ExternalTaskSensor.
        
        Args:
            session: SQLAlchemy session
            
        Returns:
            List of cross-DAG dependencies
        """
        from airflow.models import DagBag
        
        try:
            dagbag = DagBag(read_dags_from_db=True)
        except Exception as e:
            logger.error(f"Failed to load DagBag: {e}")
            return []
        
        dependencies = []
        
        for dag_id, dag in dagbag.dags.items():
            for task in dag.tasks:
                # Check for ExternalTaskSensor
                if task.task_type == "ExternalTaskSensor":
                    try:
                        external_dag_id = getattr(task, 'external_dag_id', None)
                        external_task_id = getattr(task, 'external_task_id', None)
                        
                        if external_dag_id:
                            dependencies.append({
                                "dag_id": dag_id,
                                "task_id": task.task_id,
                                "depends_on_dag": external_dag_id,
                                "depends_on_task": external_task_id,
                                "dependency_type": "ExternalTaskSensor",
                            })
                    except Exception:
                        pass
                
                # Check for TriggerDagRunOperator
                if task.task_type in ("TriggerDagRunOperator", "TriggerDagRun"):
                    try:
                        trigger_dag_id = getattr(task, 'trigger_dag_id', None)
                        if trigger_dag_id:
                            dependencies.append({
                                "dag_id": dag_id,
                                "task_id": task.task_id,
                                "triggers_dag": trigger_dag_id,
                                "dependency_type": "TriggerDagRunOperator",
                            })
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
        """Get status of all tasks in a DAG run's dependency chain.
        
        Args:
            dag_id: DAG ID
            execution_date: Execution date to check
            session: SQLAlchemy session
            
        Returns:
            Dictionary with dependency chain status
        """
        # Get all task instances for this run
        task_instances = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.execution_date == execution_date,
        ).all()
        
        if not task_instances:
            return {"error": "No task instances found"}
        
        # Group by state
        state_counts = {}
        tasks_by_state = {}
        
        for ti in task_instances:
            state = str(ti.state)
            state_counts[state] = state_counts.get(state, 0) + 1
            
            if state not in tasks_by_state:
                tasks_by_state[state] = []
            tasks_by_state[state].append(ti.task_id)
        
        # Identify bottlenecks (running tasks with queued downstream)
        bottlenecks = []
        for ti in task_instances:
            if ti.state == TaskInstanceState.RUNNING:
                # Check if this is blocking others
                # This is simplified - real implementation would check actual dependencies
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
            "timestamp": datetime.utcnow().isoformat(),
        }

    @provide_session  
    def get_failure_correlation(
        self,
        lookback_hours: int = 24,
        session: Session = None,
    ) -> Dict:
        """Find DAGs that often fail together (potential shared dependencies).
        
        Args:
            lookback_hours: Hours to analyze
            session: SQLAlchemy session
            
        Returns:
            Dictionary with failure correlations
        """
        cutoff = datetime.utcnow() - timedelta(hours=lookback_hours)
        
        # Get failed DAG runs with timestamps
        failed_runs = session.query(DagRun).filter(
            DagRun.state == DagRunState.FAILED,
            DagRun.end_date >= cutoff,
        ).order_by(DagRun.end_date).all()
        
        if len(failed_runs) < 2:
            return {
                "period_hours": lookback_hours,
                "correlations": [],
                "message": "Not enough failures to analyze correlations",
            }
        
        # Find failures that happened within 10 minutes of each other
        correlations = []
        window_minutes = 10
        
        for i, run1 in enumerate(failed_runs):
            for run2 in failed_runs[i+1:]:
                if run1.dag_id != run2.dag_id:
                    time_diff = abs((run1.end_date - run2.end_date).total_seconds() / 60)
                    
                    if time_diff <= window_minutes:
                        # Found potential correlation
                        pair = tuple(sorted([run1.dag_id, run2.dag_id]))
                        
                        # Check if we already have this pair
                        existing = next(
                            (c for c in correlations if tuple(sorted([c["dag_1"], c["dag_2"]])) == pair),
                            None
                        )
                        
                        if existing:
                            existing["co_failure_count"] += 1
                        else:
                            correlations.append({
                                "dag_1": run1.dag_id,
                                "dag_2": run2.dag_id,
                                "co_failure_count": 1,
                                "last_co_failure": max(run1.end_date, run2.end_date).isoformat(),
                            })
        
        return {
            "period_hours": lookback_hours,
            "window_minutes": window_minutes,
            "correlations": sorted(correlations, key=lambda x: x["co_failure_count"], reverse=True)[:20],
            "timestamp": datetime.utcnow().isoformat(),
        }
