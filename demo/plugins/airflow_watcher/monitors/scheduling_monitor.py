"""Scheduling Monitor - Tracks DAG scheduling lag and queue health."""

from datetime import datetime, timedelta
from typing import List, Optional, Dict
import logging
from airflow.utils import timezone

from airflow.models import DagRun, DagModel, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session
from sqlalchemy import func

from airflow_watcher.config import WatcherConfig


logger = logging.getLogger(__name__)


class SchedulingMonitor:
    """Monitor for tracking scheduling lag and queue health."""

    def __init__(self, config: Optional[WatcherConfig] = None):
        """Initialize the scheduling monitor."""
        self.config = config or WatcherConfig()

    @provide_session
    def get_scheduling_lag(
        self,
        lookback_hours: int = 24,
        lag_threshold_minutes: int = 10,
        session: Session = None,
    ) -> Dict:
        """Analyze scheduling lag across DAGs.
        
        Scheduling lag = time between scheduled execution time and actual start.
        
        Args:
            lookback_hours: Hours to analyze
            lag_threshold_minutes: Threshold to flag as delayed
            session: SQLAlchemy session
            
        Returns:
            Dictionary with scheduling lag analysis
        """
        cutoff_time = timezone.utcnow() - timedelta(hours=lookback_hours)
        
        # Get DAG runs with both scheduled time and start time
        query = session.query(DagRun).filter(
            DagRun.start_date >= cutoff_time,
            DagRun.execution_date.isnot(None),
            DagRun.start_date.isnot(None),
            DagRun.run_type == "scheduled",  # Only scheduled runs, not manual
        )
        
        lags = []
        delayed_dags = []
        
        for dag_run in query.all():
            # Calculate lag (start_date - execution_date)
            lag_seconds = (dag_run.start_date - dag_run.execution_date).total_seconds()
            lag_minutes = lag_seconds / 60
            
            lags.append(lag_minutes)
            
            if lag_minutes > lag_threshold_minutes:
                delayed_dags.append({
                    "dag_id": dag_run.dag_id,
                    "run_id": dag_run.run_id,
                    "scheduled_time": dag_run.execution_date.isoformat(),
                    "actual_start": dag_run.start_date.isoformat(),
                    "lag_minutes": round(lag_minutes, 2),
                })
        
        if not lags:
            return {
                "period_hours": lookback_hours,
                "total_runs": 0,
                "message": "No scheduled DAG runs found in the period",
            }
        
        # Calculate statistics
        avg_lag = sum(lags) / len(lags)
        max_lag = max(lags)
        min_lag = min(lags)
        
        sorted_lags = sorted(lags)
        p50_idx = int(len(sorted_lags) * 0.5)
        p90_idx = int(len(sorted_lags) * 0.9)
        p95_idx = min(int(len(sorted_lags) * 0.95), len(sorted_lags) - 1)
        
        return {
            "period_hours": lookback_hours,
            "total_runs": len(lags),
            "lag_threshold_minutes": lag_threshold_minutes,
            "delayed_count": len(delayed_dags),
            "delay_rate": round(len(delayed_dags) / len(lags) * 100, 2),
            "lag_stats": {
                "avg_minutes": round(avg_lag, 2),
                "min_minutes": round(min_lag, 2),
                "max_minutes": round(max_lag, 2),
                "p50_minutes": round(sorted_lags[p50_idx], 2),
                "p90_minutes": round(sorted_lags[p90_idx], 2),
                "p95_minutes": round(sorted_lags[p95_idx], 2),
            },
            "delayed_dags": sorted(delayed_dags, key=lambda x: x["lag_minutes"], reverse=True)[:20],
            "timestamp": timezone.utcnow().isoformat(),
        }

    @provide_session
    def get_queued_tasks(self, session: Session = None) -> Dict:
        """Get tasks currently waiting in queue.
        
        Args:
            session: SQLAlchemy session
            
        Returns:
            Dictionary with queue status
        """
        # Tasks in queued state
        queued = session.query(TaskInstance).filter(
            TaskInstance.state == TaskInstanceState.QUEUED,
        ).all()
        
        # Tasks in scheduled state (waiting to be picked up)
        scheduled = session.query(TaskInstance).filter(
            TaskInstance.state == TaskInstanceState.SCHEDULED,
        ).all()
        
        current_time = timezone.utcnow()
        
        queued_details = []
        for ti in queued:
            wait_time = None
            if ti.queued_dttm:
                wait_time = (current_time - ti.queued_dttm).total_seconds() / 60
            
            queued_details.append({
                "dag_id": ti.dag_id,
                "task_id": ti.task_id,
                "run_id": ti.run_id,
                "queued_at": ti.queued_dttm.isoformat() if ti.queued_dttm else None,
                "wait_minutes": round(wait_time, 2) if wait_time else None,
                "pool": ti.pool,
                "priority_weight": ti.priority_weight,
            })
        
        scheduled_details = []
        for ti in scheduled:
            scheduled_details.append({
                "dag_id": ti.dag_id,
                "task_id": ti.task_id,
                "run_id": ti.run_id,
                "pool": ti.pool,
            })
        
        # Sort by wait time
        queued_details = sorted(
            queued_details,
            key=lambda x: x["wait_minutes"] or 0,
            reverse=True
        )
        
        return {
            "queued_count": len(queued),
            "scheduled_count": len(scheduled),
            "total_waiting": len(queued) + len(scheduled),
            "queued_tasks": queued_details[:30],
            "scheduled_tasks": scheduled_details[:30],
            "longest_wait_minutes": queued_details[0]["wait_minutes"] if queued_details else 0,
            "timestamp": timezone.utcnow().isoformat(),
        }

    @provide_session
    def get_pool_utilization(self, session: Session = None) -> List[Dict]:
        """Get pool utilization across all pools.
        
        Args:
            session: SQLAlchemy session
            
        Returns:
            List of pool utilization details
        """
        from airflow.models import Pool
        
        pools = session.query(Pool).all()
        
        pool_stats = []
        for pool in pools:
            # Count running tasks in this pool
            running = session.query(TaskInstance).filter(
                TaskInstance.pool == pool.pool,
                TaskInstance.state == TaskInstanceState.RUNNING,
            ).count()
            
            # Count queued tasks in this pool
            queued = session.query(TaskInstance).filter(
                TaskInstance.pool == pool.pool,
                TaskInstance.state == TaskInstanceState.QUEUED,
            ).count()
            
            utilization = (running / pool.slots * 100) if pool.slots > 0 else 0
            
            pool_stats.append({
                "pool_name": pool.pool,
                "slots": pool.slots,
                "running": running,
                "queued": queued,
                "available": max(0, pool.slots - running),
                "utilization_percent": round(utilization, 2),
                "is_saturated": running >= pool.slots,
                "description": pool.description,
            })
        
        return sorted(pool_stats, key=lambda x: x["utilization_percent"], reverse=True)

    @provide_session
    def get_stale_dags(
        self,
        expected_interval_hours: int = 24,
        session: Session = None,
    ) -> List[Dict]:
        """Find DAGs that haven't run when expected.
        
        Args:
            expected_interval_hours: Expected max hours between runs
            session: SQLAlchemy session
            
        Returns:
            List of stale DAGs
        """
        cutoff_time = timezone.utcnow() - timedelta(hours=expected_interval_hours)
        
        # Get all active DAGs
        active_dags = session.query(DagModel).filter(
            DagModel.is_paused == False,
            DagModel.is_active == True,
        ).all()
        
        stale_dags = []
        current_time = timezone.utcnow()
        
        for dag in active_dags:
            # Get the most recent run
            latest_run = session.query(DagRun).filter(
                DagRun.dag_id == dag.dag_id,
            ).order_by(DagRun.execution_date.desc()).first()
            
            if not latest_run:
                # Never run
                stale_dags.append({
                    "dag_id": dag.dag_id,
                    "last_run": None,
                    "hours_since_run": None,
                    "status": "never_run",
                    "schedule_interval": str(dag.schedule_interval),
                })
            elif latest_run.execution_date < cutoff_time:
                hours_since = (current_time - latest_run.execution_date).total_seconds() / 3600
                stale_dags.append({
                    "dag_id": dag.dag_id,
                    "last_run": latest_run.execution_date.isoformat(),
                    "last_run_state": str(latest_run.state),
                    "hours_since_run": round(hours_since, 2),
                    "status": "stale",
                    "schedule_interval": str(dag.schedule_interval),
                })
        
        return sorted(stale_dags, key=lambda x: x.get("hours_since_run") or 999999, reverse=True)

    @provide_session
    def get_concurrent_runs(self, session: Session = None) -> Dict:
        """Get DAGs with multiple concurrent runs (potential issues).
        
        Args:
            session: SQLAlchemy session
            
        Returns:
            Dictionary with concurrent run information
        """
        # Find DAGs with multiple running instances
        concurrent = session.query(
            DagRun.dag_id,
            func.count(DagRun.dag_id).label("running_count"),
        ).filter(
            DagRun.state == DagRunState.RUNNING,
        ).group_by(DagRun.dag_id).having(
            func.count(DagRun.dag_id) > 1
        ).all()
        
        concurrent_dags = []
        for dag_id, count in concurrent:
            # Get the running instances
            runs = session.query(DagRun).filter(
                DagRun.dag_id == dag_id,
                DagRun.state == DagRunState.RUNNING,
            ).order_by(DagRun.execution_date.desc()).all()
            
            concurrent_dags.append({
                "dag_id": dag_id,
                "running_count": count,
                "runs": [
                    {
                        "run_id": r.run_id,
                        "execution_date": r.execution_date.isoformat(),
                        "start_date": r.start_date.isoformat() if r.start_date else None,
                    }
                    for r in runs
                ],
            })
        
        return {
            "dags_with_concurrent_runs": len(concurrent_dags),
            "concurrent_dags": concurrent_dags,
            "timestamp": timezone.utcnow().isoformat(),
        }
