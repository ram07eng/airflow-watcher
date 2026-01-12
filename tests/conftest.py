"""Pytest configuration for Airflow Watcher tests."""

import pytest
import sys
from pathlib import Path

# Add src to path for imports
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))


@pytest.fixture
def sample_datetime():
    """Provide a sample datetime for tests."""
    from datetime import datetime
    return datetime(2024, 1, 15, 10, 30, 0)


@pytest.fixture
def mock_airflow_config(monkeypatch):
    """Mock Airflow configuration."""
    def mock_get(section, key, fallback=None):
        return fallback
    
    def mock_getint(section, key, fallback=None):
        return fallback
    
    def mock_getboolean(section, key, fallback=None):
        return fallback
    
    # Note: In real tests, we'd patch airflow.configuration.conf
    pass


@pytest.fixture
def sample_dag_failure(sample_datetime):
    """Provide a sample DAGFailure for tests."""
    from airflow_watcher.models.failure import DAGFailure, TaskFailure
    
    return DAGFailure(
        dag_id="sample_dag",
        run_id="manual__2024-01-15",
        execution_date=sample_datetime,
        start_date=sample_datetime,
        end_date=sample_datetime,
        state="failed",
        failed_tasks=[
            TaskFailure(
                task_id="task_1",
                dag_id="sample_dag",
                run_id="manual__2024-01-15",
                execution_date=sample_datetime,
                state="failed",
            )
        ],
    )


@pytest.fixture
def sample_sla_miss(sample_datetime):
    """Provide a sample SLAMissEvent for tests."""
    from airflow_watcher.models.sla import SLAMissEvent
    
    return SLAMissEvent(
        dag_id="sample_dag",
        task_id="sample_task",
        execution_date=sample_datetime,
        timestamp=sample_datetime,
        email_sent=False,
        notification_sent=False,
    )
