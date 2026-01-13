"""DAG Failure Monitor - Tracks and reports DAG/task failures."""

from datetime import datetime, timedelta
from typing import List, Optional
import logging
from airflow.utils import timezone

from airflow.models import DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session

from airflow_watcher.models.failure import DAGFailure, TaskFailure
from airflow_watcher.config import WatcherConfig


logger = logging.getLogger(__name__)


class DAGFailureMonitor:
    """Monitor for tracking DAG and task failures."""

    def __init__(self, config: Optional[WatcherConfig] = None):
        """Initialize the DAG failure monitor.
        
        Args:
            config: Optional configuration object. Uses defaults if not provided.
        """
        self.config = config or WatcherConfig()

    @provide_session
    def get_recent_failures(
        self,
        dag_id: Optional[str] = None,
        lookback_hours: int = 24,
        limit: int = 50,
        session: Session = None,
    ) -> List[DAGFailure]:
        """Get recent DAG failures.
        
        Args:
            dag_id: Optional DAG ID to filter by
            lookback_hours: Hours to look back for failures
            limit: Maximum number of failures to return
            session: SQLAlchemy session
            
        Returns:
            List of DAGFailure objects
        """
        cutoff_time = timezone.utcnow() - timedelta(hours=lookback_hours)
        
        query = session.query(DagRun).filter(
            DagRun.state == DagRunState.FAILED,
            DagRun.end_date >= cutoff_time,
        )
        
        if dag_id:
            query = query.filter(DagRun.dag_id == dag_id)
        
        query = query.order_by(DagRun.end_date.desc()).limit(limit)
        
        failures = []
        for dag_run in query.all():
            # Get failed tasks for this run
            failed_tasks = self._get_failed_tasks(dag_run, session)
            
            failure = DAGFailure(
                dag_id=dag_run.dag_id,
                run_id=dag_run.run_id,
                execution_date=dag_run.execution_date,
                start_date=dag_run.start_date,
                end_date=dag_run.end_date,
                state=str(dag_run.state),
                failed_tasks=failed_tasks,
                external_trigger=dag_run.external_trigger,
            )
            failures.append(failure)
        
        return failures

    def _get_failed_tasks(self, dag_run: DagRun, session: Session) -> List[TaskFailure]:
        """Get failed tasks for a specific DAG run.
        
        Args:
            dag_run: The DAG run to get failed tasks for
            session: SQLAlchemy session
            
        Returns:
            List of TaskFailure objects
        """
        query = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_run.dag_id,
            TaskInstance.run_id == dag_run.run_id,
            TaskInstance.state == TaskInstanceState.FAILED,
        )
        
        failed_tasks = []
        for ti in query.all():
            task_failure = TaskFailure(
                task_id=ti.task_id,
                dag_id=ti.dag_id,
                run_id=ti.run_id,
                execution_date=ti.execution_date,
                start_date=ti.start_date,
                end_date=ti.end_date,
                duration=ti.duration,
                state=str(ti.state),
                try_number=ti.try_number,
                max_tries=ti.max_tries,
                operator=ti.operator,
            )
            failed_tasks.append(task_failure)
        
        return failed_tasks

    @provide_session
    def get_failure_statistics(
        self,
        lookback_hours: int = 24,
        session: Session = None,
    ) -> dict:
        """Get failure statistics for the specified time period.
        
        Args:
            lookback_hours: Hours to look back
            session: SQLAlchemy session
            
        Returns:
            Dictionary with failure statistics
        """
        cutoff_time = timezone.utcnow() - timedelta(hours=lookback_hours)
        
        # Total DAG runs
        total_runs = session.query(DagRun).filter(
            DagRun.end_date >= cutoff_time,
        ).count()
        
        # Failed DAG runs
        failed_runs = session.query(DagRun).filter(
            DagRun.state == DagRunState.FAILED,
            DagRun.end_date >= cutoff_time,
        ).count()
        
        # Successful DAG runs
        success_runs = session.query(DagRun).filter(
            DagRun.state == DagRunState.SUCCESS,
            DagRun.end_date >= cutoff_time,
        ).count()
        
        # Total task failures
        total_task_failures = session.query(TaskInstance).filter(
            TaskInstance.state == TaskInstanceState.FAILED,
            TaskInstance.end_date >= cutoff_time,
        ).count()
        
        # Failure rate
        failure_rate = (failed_runs / total_runs * 100) if total_runs > 0 else 0
        
        # Most failing DAGs
        from sqlalchemy import func
        most_failing = session.query(
            DagRun.dag_id,
            func.count(DagRun.dag_id).label("failure_count")
        ).filter(
            DagRun.state == DagRunState.FAILED,
            DagRun.end_date >= cutoff_time,
        ).group_by(DagRun.dag_id).order_by(
            func.count(DagRun.dag_id).desc()
        ).limit(10).all()
        
        return {
            "period_hours": lookback_hours,
            "total_runs": total_runs,
            "failed_runs": failed_runs,
            "success_runs": success_runs,
            "failure_rate": round(failure_rate, 2),
            "total_task_failures": total_task_failures,
            "most_failing_dags": [
                {"dag_id": dag_id, "failure_count": count}
                for dag_id, count in most_failing
            ],
            "timestamp": timezone.utcnow().isoformat(),
        }

    @provide_session
    def get_dag_health_status(self, session: Session = None) -> dict:
        """Get overall health status of all DAGs.
        
        Args:
            session: SQLAlchemy session
            
        Returns:
            Dictionary with health status per DAG
        """
        from sqlalchemy import func, case
        
        # Get the latest run status for each DAG
        subquery = session.query(
            DagRun.dag_id,
            func.max(DagRun.execution_date).label("latest_execution")
        ).group_by(DagRun.dag_id).subquery()
        
        latest_runs = session.query(DagRun).join(
            subquery,
            (DagRun.dag_id == subquery.c.dag_id) &
            (DagRun.execution_date == subquery.c.latest_execution)
        ).all()
        
        health_status = {
            "healthy": [],
            "unhealthy": [],
            "running": [],
            "unknown": [],
        }
        
        for dag_run in latest_runs:
            dag_info = {
                "dag_id": dag_run.dag_id,
                "state": str(dag_run.state),
                "last_run": dag_run.execution_date.isoformat() if dag_run.execution_date else None,
            }
            
            if dag_run.state == DagRunState.SUCCESS:
                health_status["healthy"].append(dag_info)
            elif dag_run.state == DagRunState.FAILED:
                health_status["unhealthy"].append(dag_info)
            elif dag_run.state == DagRunState.RUNNING:
                health_status["running"].append(dag_info)
            else:
                health_status["unknown"].append(dag_info)
        
        health_status["summary"] = {
            "healthy_count": len(health_status["healthy"]),
            "unhealthy_count": len(health_status["unhealthy"]),
            "running_count": len(health_status["running"]),
            "unknown_count": len(health_status["unknown"]),
            "total": len(latest_runs),
        }
        
        return health_status
