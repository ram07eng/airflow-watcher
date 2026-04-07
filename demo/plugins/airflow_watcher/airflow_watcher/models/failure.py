"""Data models for DAG and task failures."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class TaskFailure:
    """Represents a task failure."""

    task_id: str
    dag_id: str
    run_id: str
    execution_date: datetime
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    duration: Optional[float] = None
    state: str = "failed"
    try_number: int = 1
    max_tries: int = 1
    operator: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "task_id": self.task_id,
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "execution_date": self.execution_date.isoformat() if self.execution_date else None,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "duration": self.duration,
            "state": self.state,
            "try_number": self.try_number,
            "max_tries": self.max_tries,
            "operator": self.operator,
        }


@dataclass
class DAGFailure:
    """Represents a DAG failure."""

    dag_id: str
    run_id: str
    execution_date: datetime
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    state: str = "failed"
    failed_tasks: List[TaskFailure] = field(default_factory=list)
    external_trigger: bool = False

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "execution_date": self.execution_date.isoformat() if self.execution_date else None,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "state": self.state,
            "failed_tasks": [t.to_dict() for t in self.failed_tasks],
            "failed_task_count": len(self.failed_tasks),
            "external_trigger": self.external_trigger,
        }

    @property
    def duration(self) -> Optional[float]:
        """Get duration in seconds."""
        if self.start_date and self.end_date:
            return (self.end_date - self.start_date).total_seconds()
        return None
