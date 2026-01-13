"""DAG Health Monitor - Tracks DAG parse errors and import issues."""

from datetime import datetime, timedelta
from typing import List, Optional, Dict
import logging
from airflow.utils import timezone

from airflow.models import DagModel, DagRun, ImportError as AirflowImportError
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session
from sqlalchemy import func

from airflow_watcher.config import WatcherConfig


logger = logging.getLogger(__name__)


class DAGHealthMonitor:
    """Monitor for tracking DAG-level health issues."""

    def __init__(self, config: Optional[WatcherConfig] = None):
        """Initialize the DAG health monitor."""
        self.config = config or WatcherConfig()

    @provide_session
    def get_dag_import_errors(self, session: Session = None) -> List[Dict]:
        """Get DAGs with import/parse errors.
        
        Args:
            session: SQLAlchemy session
            
        Returns:
            List of DAGs with import errors
        """
        errors = session.query(AirflowImportError).all()
        
        import_errors = []
        for error in errors:
            import_errors.append({
                "filename": error.filename,
                "timestamp": error.timestamp.isoformat() if error.timestamp else None,
                "stacktrace": error.stacktrace[:500] if error.stacktrace else None,  # Truncate
                "full_stacktrace": error.stacktrace,
            })
        
        return import_errors

    @provide_session
    def get_dag_status_summary(self, session: Session = None) -> Dict:
        """Get overall DAG status summary.
        
        Args:
            session: SQLAlchemy session
            
        Returns:
            Dictionary with DAG status summary
        """
        # Total DAGs
        total_dags = session.query(DagModel).count()
        
        # Active DAGs
        active_dags = session.query(DagModel).filter(
            DagModel.is_active == True,
        ).count()
        
        # Paused DAGs
        paused_dags = session.query(DagModel).filter(
            DagModel.is_paused == True,
        ).count()
        
        # DAGs with import errors
        import_error_count = session.query(AirflowImportError).count()
        
        # Running DAG runs
        running_runs = session.query(DagRun).filter(
            DagRun.state == "running",
        ).count()
        
        # Recent failures (24h)
        cutoff = timezone.utcnow() - timedelta(hours=24)
        recent_failures = session.query(DagRun).filter(
            DagRun.state == "failed",
            DagRun.end_date >= cutoff,
        ).count()
        
        return {
            "total_dags": total_dags,
            "active_dags": active_dags,
            "paused_dags": paused_dags,
            "dags_with_errors": import_error_count,
            "currently_running": running_runs,
            "recent_failures_24h": recent_failures,
            "health_score": self._calculate_health_score(
                total_dags, import_error_count, recent_failures
            ),
            "timestamp": timezone.utcnow().isoformat(),
        }

    def _calculate_health_score(
        self,
        total_dags: int,
        error_count: int,
        failure_count: int,
    ) -> int:
        """Calculate overall health score (0-100).
        
        Args:
            total_dags: Total number of DAGs
            error_count: Number of DAGs with errors
            failure_count: Number of recent failures
            
        Returns:
            Health score from 0-100
        """
        if total_dags == 0:
            return 100
        
        # Start with 100
        score = 100
        
        # Deduct for import errors (10 points each, max 30)
        score -= min(error_count * 10, 30)
        
        # Deduct for failures (2 points each, max 40)
        score -= min(failure_count * 2, 40)
        
        return max(0, score)

    @provide_session
    def get_dag_complexity_analysis(self, session: Session = None) -> List[Dict]:
        """Analyze DAG complexity metrics.
        
        Args:
            session: SQLAlchemy session
            
        Returns:
            List of DAGs with complexity metrics
        """
        from airflow.models import DagBag
        
        try:
            dagbag = DagBag(read_dags_from_db=True)
        except Exception as e:
            logger.error(f"Failed to load DagBag: {e}")
            return []
        
        complexity_data = []
        
        for dag_id, dag in dagbag.dags.items():
            task_count = len(dag.tasks)
            
            # Count dependencies
            dependency_count = sum(len(task.upstream_list) for task in dag.tasks)
            
            # Calculate depth (longest path)
            max_depth = self._calculate_dag_depth(dag)
            
            complexity_data.append({
                "dag_id": dag_id,
                "task_count": task_count,
                "dependency_count": dependency_count,
                "max_depth": max_depth,
                "avg_dependencies_per_task": round(dependency_count / task_count, 2) if task_count > 0 else 0,
                "schedule_interval": str(dag.schedule_interval),
                "is_paused": dag.is_paused,
                "tags": [t.name for t in dag.tags] if dag.tags else [],
            })
        
        return sorted(complexity_data, key=lambda x: x["task_count"], reverse=True)

    def _calculate_dag_depth(self, dag) -> int:
        """Calculate the maximum depth of a DAG."""
        if not dag.tasks:
            return 0
        
        # Find root tasks (no upstream)
        roots = [t for t in dag.tasks if not t.upstream_list]
        
        if not roots:
            return 0
        
        def get_depth(task, visited=None):
            if visited is None:
                visited = set()
            
            if task.task_id in visited:
                return 0  # Cycle detection
            
            visited.add(task.task_id)
            
            if not task.downstream_list:
                return 1
            
            return 1 + max(
                get_depth(downstream, visited.copy())
                for downstream in task.downstream_list
            )
        
        return max(get_depth(root) for root in roots)

    @provide_session
    def get_inactive_dags(
        self,
        inactive_days: int = 30,
        session: Session = None,
    ) -> List[Dict]:
        """Find DAGs that haven't been modified or run recently.
        
        Args:
            inactive_days: Days of inactivity threshold
            session: SQLAlchemy session
            
        Returns:
            List of inactive DAGs
        """
        cutoff = timezone.utcnow() - timedelta(days=inactive_days)
        
        # Get DAGs that haven't run recently
        inactive_dags = []
        
        all_dags = session.query(DagModel).filter(
            DagModel.is_active == True,
        ).all()
        
        for dag in all_dags:
            # Get most recent run
            latest_run = session.query(DagRun).filter(
                DagRun.dag_id == dag.dag_id,
            ).order_by(DagRun.execution_date.desc()).first()
            
            last_run_date = latest_run.execution_date if latest_run else None
            
            if not latest_run or latest_run.execution_date < cutoff:
                days_inactive = None
                if latest_run:
                    days_inactive = (timezone.utcnow() - latest_run.execution_date).days
                
                inactive_dags.append({
                    "dag_id": dag.dag_id,
                    "last_run": last_run_date.isoformat() if last_run_date else None,
                    "days_inactive": days_inactive,
                    "is_paused": dag.is_paused,
                    "schedule_interval": str(dag.schedule_interval) if dag.schedule_interval else None,
                    "recommendation": "Consider pausing or removing" if days_inactive and days_inactive > 60 else "Review",
                })
        
        return sorted(
            inactive_dags,
            key=lambda x: x["days_inactive"] or 999,
            reverse=True
        )
