"""Data models for SLA events."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class SLAMissEvent:
    """Represents an SLA miss event."""
    
    dag_id: str
    task_id: str
    execution_date: datetime
    timestamp: datetime
    email_sent: bool = False
    notification_sent: bool = False
    description: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "execution_date": self.execution_date.isoformat() if self.execution_date else None,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "email_sent": self.email_sent,
            "notification_sent": self.notification_sent,
            "description": self.description,
        }


@dataclass
class SLADelayWarning:
    """Represents a warning for potential SLA delay."""
    
    dag_id: str
    run_id: str
    execution_date: datetime
    start_date: datetime
    running_minutes: float
    threshold_minutes: float
    risk_level: str  # "low", "medium", "high"
    estimated_completion: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "execution_date": self.execution_date.isoformat() if self.execution_date else None,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "running_minutes": self.running_minutes,
            "threshold_minutes": self.threshold_minutes,
            "risk_level": self.risk_level,
            "estimated_completion": self.estimated_completion.isoformat() if self.estimated_completion else None,
        }
