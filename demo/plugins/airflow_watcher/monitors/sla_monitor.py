"""SLA Monitor - Tracks SLA misses and delays."""

from datetime import datetime, timedelta
from typing import List, Optional
import logging
from airflow.utils import timezone

from airflow.models import SlaMiss, DagRun, TaskInstance
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session
from sqlalchemy import func

from airflow_watcher.models.sla import SLAMissEvent
from airflow_watcher.config import WatcherConfig


logger = logging.getLogger(__name__)


class SLAMonitor:
    """Monitor for tracking SLA misses and delays."""

    def __init__(self, config: Optional[WatcherConfig] = None):
        """Initialize the SLA monitor.
        
        Args:
            config: Optional configuration object. Uses defaults if not provided.
        """
        self.config = config or WatcherConfig()

    @provide_session
    def get_recent_sla_misses(
        self,
        dag_id: Optional[str] = None,
        task_id: Optional[str] = None,
        lookback_hours: int = 24,
        limit: int = 50,
        session: Session = None,
    ) -> List[SLAMissEvent]:
        """Get recent SLA misses.
        
        Args:
            dag_id: Optional DAG ID to filter by
            task_id: Optional task ID to filter by
            lookback_hours: Hours to look back for SLA misses
            limit: Maximum number of misses to return
            session: SQLAlchemy session
            
        Returns:
            List of SLAMissEvent objects
        """
        cutoff_time = timezone.utcnow() - timedelta(hours=lookback_hours)
        
        query = session.query(SlaMiss).filter(
            SlaMiss.timestamp >= cutoff_time,
        )
        
        if dag_id:
            query = query.filter(SlaMiss.dag_id == dag_id)
        
        if task_id:
            query = query.filter(SlaMiss.task_id == task_id)
        
        query = query.order_by(SlaMiss.timestamp.desc()).limit(limit)
        
        sla_misses = []
        for sla_miss in query.all():
            event = SLAMissEvent(
                dag_id=sla_miss.dag_id,
                task_id=sla_miss.task_id,
                execution_date=sla_miss.execution_date,
                timestamp=sla_miss.timestamp,
                email_sent=sla_miss.email_sent,
                notification_sent=sla_miss.notification_sent,
                description=sla_miss.description,
            )
            sla_misses.append(event)
        
        return sla_misses

    @provide_session
    def get_sla_statistics(
        self,
        lookback_hours: int = 24,
        session: Session = None,
    ) -> dict:
        """Get SLA miss statistics for the specified time period.
        
        Args:
            lookback_hours: Hours to look back
            session: SQLAlchemy session
            
        Returns:
            Dictionary with SLA statistics
        """
        cutoff_time = timezone.utcnow() - timedelta(hours=lookback_hours)
        
        # Total SLA misses
        total_misses = session.query(SlaMiss).filter(
            SlaMiss.timestamp >= cutoff_time,
        ).count()
        
        # SLA misses by DAG
        misses_by_dag = session.query(
            SlaMiss.dag_id,
            func.count(SlaMiss.dag_id).label("miss_count")
        ).filter(
            SlaMiss.timestamp >= cutoff_time,
        ).group_by(SlaMiss.dag_id).order_by(
            func.count(SlaMiss.dag_id).desc()
        ).limit(10).all()
        
        # SLA misses by task
        misses_by_task = session.query(
            SlaMiss.dag_id,
            SlaMiss.task_id,
            func.count(SlaMiss.task_id).label("miss_count")
        ).filter(
            SlaMiss.timestamp >= cutoff_time,
        ).group_by(SlaMiss.dag_id, SlaMiss.task_id).order_by(
            func.count(SlaMiss.task_id).desc()
        ).limit(10).all()
        
        # Notifications sent
        notifications_sent = session.query(SlaMiss).filter(
            SlaMiss.timestamp >= cutoff_time,
            SlaMiss.notification_sent == True,
        ).count()
        
        # Emails sent
        emails_sent = session.query(SlaMiss).filter(
            SlaMiss.timestamp >= cutoff_time,
            SlaMiss.email_sent == True,
        ).count()
        
        return {
            "period_hours": lookback_hours,
            "total_sla_misses": total_misses,
            "notifications_sent": notifications_sent,
            "emails_sent": emails_sent,
            "top_dags_with_misses": [
                {"dag_id": dag_id, "miss_count": count}
                for dag_id, count in misses_by_dag
            ],
            "top_tasks_with_misses": [
                {"dag_id": dag_id, "task_id": task_id, "miss_count": count}
                for dag_id, task_id, count in misses_by_task
            ],
            "timestamp": timezone.utcnow().isoformat(),
        }

    @provide_session
    def get_dag_delay_analysis(
        self,
        dag_id: str,
        lookback_days: int = 7,
        session: Session = None,
    ) -> dict:
        """Analyze delays for a specific DAG.
        
        Args:
            dag_id: DAG ID to analyze
            lookback_days: Days to look back for analysis
            session: SQLAlchemy session
            
        Returns:
            Dictionary with delay analysis
        """
        cutoff_time = timezone.utcnow() - timedelta(days=lookback_days)
        
        # Get all completed runs for the DAG
        dag_runs = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.end_date >= cutoff_time,
            DagRun.end_date.isnot(None),
        ).all()
        
        if not dag_runs:
            return {
                "dag_id": dag_id,
                "analysis_period_days": lookback_days,
                "total_runs": 0,
                "message": "No completed runs found for the specified period",
            }
        
        durations = []
        for run in dag_runs:
            if run.start_date and run.end_date:
                duration = (run.end_date - run.start_date).total_seconds()
                durations.append(duration)
        
        if not durations:
            return {
                "dag_id": dag_id,
                "analysis_period_days": lookback_days,
                "total_runs": len(dag_runs),
                "message": "No duration data available",
            }
        
        avg_duration = sum(durations) / len(durations)
        min_duration = min(durations)
        max_duration = max(durations)
        
        # Calculate percentiles
        sorted_durations = sorted(durations)
        p50_idx = int(len(sorted_durations) * 0.5)
        p90_idx = int(len(sorted_durations) * 0.9)
        p95_idx = int(len(sorted_durations) * 0.95)
        
        # SLA misses for this DAG
        sla_miss_count = session.query(SlaMiss).filter(
            SlaMiss.dag_id == dag_id,
            SlaMiss.timestamp >= cutoff_time,
        ).count()
        
        return {
            "dag_id": dag_id,
            "analysis_period_days": lookback_days,
            "total_runs": len(dag_runs),
            "duration_stats": {
                "avg_seconds": round(avg_duration, 2),
                "min_seconds": round(min_duration, 2),
                "max_seconds": round(max_duration, 2),
                "p50_seconds": round(sorted_durations[p50_idx], 2),
                "p90_seconds": round(sorted_durations[p90_idx], 2),
                "p95_seconds": round(sorted_durations[p95_idx], 2),
            },
            "sla_misses": sla_miss_count,
            "timestamp": timezone.utcnow().isoformat(),
        }

    @provide_session
    def check_running_dags_for_sla_risk(
        self,
        sla_threshold_minutes: int = 30,
        session: Session = None,
    ) -> List[dict]:
        """Check currently running DAGs for SLA risk.
        
        Args:
            sla_threshold_minutes: Minutes threshold to flag as at risk
            session: SQLAlchemy session
            
        Returns:
            List of DAGs at risk of SLA miss
        """
        from airflow.utils.state import DagRunState
        
        # Get currently running DAG runs
        running_dags = session.query(DagRun).filter(
            DagRun.state == DagRunState.RUNNING,
        ).all()
        
        at_risk = []
        current_time = timezone.utcnow()
        
        for dag_run in running_dags:
            if dag_run.start_date:
                running_time = (current_time - dag_run.start_date).total_seconds() / 60
                
                # Check if running longer than threshold
                if running_time > sla_threshold_minutes:
                    at_risk.append({
                        "dag_id": dag_run.dag_id,
                        "run_id": dag_run.run_id,
                        "execution_date": dag_run.execution_date.isoformat(),
                        "start_date": dag_run.start_date.isoformat(),
                        "running_minutes": round(running_time, 2),
                        "threshold_minutes": sla_threshold_minutes,
                        "risk_level": "high" if running_time > sla_threshold_minutes * 2 else "medium",
                    })
        
        return at_risk
