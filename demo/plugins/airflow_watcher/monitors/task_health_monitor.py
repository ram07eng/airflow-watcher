"""Task Health Monitor - Tracks long-running tasks, retries, and zombies."""

from datetime import datetime, timedelta
from typing import List, Optional, Dict
import logging
from airflow.utils import timezone

from airflow.models import TaskInstance, DagRun
from airflow.utils.state import TaskInstanceState, DagRunState
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session
from sqlalchemy import func

from airflow_watcher.config import WatcherConfig


logger = logging.getLogger(__name__)


class TaskHealthMonitor:
    """Monitor for tracking task-level health issues."""

    def __init__(self, config: Optional[WatcherConfig] = None):
        """Initialize the task health monitor."""
        self.config = config or WatcherConfig()

    @provide_session
    def get_long_running_tasks(
        self,
        threshold_minutes: int = 60,
        session: Session = None,
    ) -> List[Dict]:
        """Get tasks running longer than the threshold.
        
        Args:
            threshold_minutes: Minutes threshold to flag as long-running
            session: SQLAlchemy session
            
        Returns:
            List of long-running task details
        """
        cutoff_time = timezone.utcnow() - timedelta(minutes=threshold_minutes)
        
        query = session.query(TaskInstance).filter(
            TaskInstance.state == TaskInstanceState.RUNNING,
            TaskInstance.start_date <= cutoff_time,
        )
        
        long_running = []
        current_time = timezone.utcnow()
        
        for ti in query.all():
            running_minutes = (current_time - ti.start_date).total_seconds() / 60
            
            long_running.append({
                "dag_id": ti.dag_id,
                "task_id": ti.task_id,
                "run_id": ti.run_id,
                "execution_date": ti.execution_date.isoformat() if ti.execution_date else None,
                "start_date": ti.start_date.isoformat() if ti.start_date else None,
                "running_minutes": round(running_minutes, 2),
                "operator": ti.operator,
                "severity": self._get_duration_severity(running_minutes, threshold_minutes),
            })
        
        return sorted(long_running, key=lambda x: x["running_minutes"], reverse=True)

    def _get_duration_severity(self, running_minutes: float, threshold: int) -> str:
        """Determine severity based on how much threshold is exceeded."""
        ratio = running_minutes / threshold
        if ratio >= 3:
            return "critical"
        elif ratio >= 2:
            return "high"
        elif ratio >= 1.5:
            return "medium"
        return "low"

    @provide_session
    def get_retry_heavy_tasks(
        self,
        lookback_hours: int = 24,
        min_retries: int = 2,
        session: Session = None,
    ) -> List[Dict]:
        """Get tasks with excessive retries.
        
        Args:
            lookback_hours: Hours to look back
            min_retries: Minimum retry count to include
            session: SQLAlchemy session
            
        Returns:
            List of tasks with high retry counts
        """
        cutoff_time = timezone.utcnow() - timedelta(hours=lookback_hours)
        
        # Get tasks that have been retried multiple times
        query = session.query(TaskInstance).filter(
            TaskInstance.end_date >= cutoff_time,
            TaskInstance.try_number > min_retries,
        ).order_by(TaskInstance.try_number.desc())
        
        retry_tasks = []
        for ti in query.limit(50).all():
            retry_tasks.append({
                "dag_id": ti.dag_id,
                "task_id": ti.task_id,
                "run_id": ti.run_id,
                "try_number": ti.try_number,
                "max_tries": ti.max_tries,
                "state": str(ti.state),
                "operator": ti.operator,
                "last_attempt": ti.end_date.isoformat() if ti.end_date else None,
            })
        
        return retry_tasks

    @provide_session
    def get_zombie_tasks(
        self,
        zombie_threshold_minutes: int = 120,
        session: Session = None,
    ) -> List[Dict]:
        """Detect potential zombie tasks (running but no heartbeat).
        
        Args:
            zombie_threshold_minutes: Minutes without update to consider zombie
            session: SQLAlchemy session
            
        Returns:
            List of potential zombie tasks
        """
        cutoff_time = timezone.utcnow() - timedelta(minutes=zombie_threshold_minutes)
        
        # Tasks marked as running but not updated recently
        query = session.query(TaskInstance).filter(
            TaskInstance.state == TaskInstanceState.RUNNING,
            TaskInstance.start_date <= cutoff_time,
        )
        
        zombies = []
        current_time = timezone.utcnow()
        
        for ti in query.all():
            # Check if the task's DAG run is still running
            dag_run = session.query(DagRun).filter(
                DagRun.dag_id == ti.dag_id,
                DagRun.run_id == ti.run_id,
            ).first()
            
            # If DAG run is not running but task is, it's likely a zombie
            is_orphaned = dag_run and dag_run.state != DagRunState.RUNNING
            
            running_minutes = (current_time - ti.start_date).total_seconds() / 60
            
            zombies.append({
                "dag_id": ti.dag_id,
                "task_id": ti.task_id,
                "run_id": ti.run_id,
                "start_date": ti.start_date.isoformat() if ti.start_date else None,
                "running_minutes": round(running_minutes, 2),
                "is_orphaned": is_orphaned,
                "dag_run_state": str(dag_run.state) if dag_run else "unknown",
                "operator": ti.operator,
            })
        
        return zombies

    @provide_session
    def get_task_failure_patterns(
        self,
        lookback_hours: int = 168,  # 7 days
        session: Session = None,
    ) -> Dict:
        """Analyze task failure patterns to identify problematic tasks.
        
        Args:
            lookback_hours: Hours to analyze
            session: SQLAlchemy session
            
        Returns:
            Dictionary with failure pattern analysis
        """
        cutoff_time = timezone.utcnow() - timedelta(hours=lookback_hours)
        
        # Get failure counts by task
        failure_counts = session.query(
            TaskInstance.dag_id,
            TaskInstance.task_id,
            func.count(TaskInstance.task_id).label("failure_count"),
        ).filter(
            TaskInstance.state == TaskInstanceState.FAILED,
            TaskInstance.end_date >= cutoff_time,
        ).group_by(
            TaskInstance.dag_id,
            TaskInstance.task_id,
        ).order_by(
            func.count(TaskInstance.task_id).desc()
        ).limit(20).all()
        
        # Get total task runs for failure rate calculation
        total_runs = session.query(TaskInstance).filter(
            TaskInstance.end_date >= cutoff_time,
        ).count()
        
        total_failures = session.query(TaskInstance).filter(
            TaskInstance.state == TaskInstanceState.FAILED,
            TaskInstance.end_date >= cutoff_time,
        ).count()
        
        # Identify flaky tasks (high failure + high retry success)
        flaky_tasks = []
        for dag_id, task_id, failure_count in failure_counts[:10]:
            success_after_retry = session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.task_id == task_id,
                TaskInstance.state == TaskInstanceState.SUCCESS,
                TaskInstance.try_number > 1,
                TaskInstance.end_date >= cutoff_time,
            ).count()
            
            if success_after_retry > 0:
                flaky_tasks.append({
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "failure_count": failure_count,
                    "retry_success_count": success_after_retry,
                    "flakiness_score": round(success_after_retry / (failure_count + success_after_retry) * 100, 2),
                })
        
        return {
            "period_hours": lookback_hours,
            "total_task_runs": total_runs,
            "total_failures": total_failures,
            "failure_rate": round(total_failures / total_runs * 100, 2) if total_runs > 0 else 0,
            "top_failing_tasks": [
                {"dag_id": d, "task_id": t, "failure_count": c}
                for d, t, c in failure_counts
            ],
            "flaky_tasks": flaky_tasks,
            "timestamp": timezone.utcnow().isoformat(),
        }
