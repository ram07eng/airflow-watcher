"""Pytest configuration for Airflow Watcher tests."""

import sys
from pathlib import Path
from datetime import datetime
from unittest.mock import MagicMock

import pytest

# Add src to path for imports
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

# ---------------------------------------------------------------------------
# Stub out heavy dependencies so tests run without a full Airflow/SQLAlchemy
# install in the test environment.
# ---------------------------------------------------------------------------

def _make_stub(name):
    stub = MagicMock()
    stub.__name__ = name
    return stub


_airflow_conf = MagicMock()
_airflow_conf.get = lambda s, k, fallback=None: fallback
_airflow_conf.getint = lambda s, k, fallback=None: fallback
_airflow_conf.getboolean = lambda s, k, fallback=None: fallback

_timezone = MagicMock()
_timezone.utcnow = datetime.utcnow

# provide_session: just call the function directly with session=None
def _provide_session(f):
    def wrapper(*args, **kwargs):
        kwargs.setdefault("session", MagicMock())
        return f(*args, **kwargs)
    wrapper.__name__ = getattr(f, "__name__", "wrapped")
    return wrapper

_STUBS = {
    "airflow": MagicMock(),
    "airflow.configuration": MagicMock(conf=_airflow_conf),
    "airflow.models": MagicMock(),
    "airflow.utils": MagicMock(timezone=_timezone),
    "airflow.utils.timezone": _timezone,
    "airflow.utils.session": MagicMock(provide_session=_provide_session),
    "airflow.utils.state": MagicMock(),
    "airflow.utils.db": MagicMock(provide_session=_provide_session),
    "airflow.plugins_manager": MagicMock(),
    "flask_appbuilder": MagicMock(),
    "sqlalchemy": MagicMock(),
    "sqlalchemy.orm": MagicMock(),
    "sqlalchemy.orm.session": MagicMock(),
    "sqlalchemy.orm.Session": MagicMock(),
    "slack_sdk": MagicMock(),
    "slack_sdk.webhook": MagicMock(),
    "slack_sdk.errors": MagicMock(),
}

for mod_name, stub in _STUBS.items():
    sys.modules.setdefault(mod_name, stub)

# Make Session importable as a class
sys.modules["sqlalchemy.orm"].Session = MagicMock


class _ColMock(MagicMock):
    """MagicMock subclass that supports all comparison operators.

    SQLAlchemy column attributes are compared with Python operators in filter()
    calls (e.g. DagRun.end_date >= cutoff). Without this, MagicMock raises
    TypeError when compared to a real datetime.
    """
    def __ge__(self, other): return MagicMock()
    def __gt__(self, other): return MagicMock()
    def __le__(self, other): return MagicMock()
    def __lt__(self, other): return MagicMock()
    def __eq__(self, other): return MagicMock()
    def __ne__(self, other): return MagicMock()


def _make_model_mock():
    """Create a MagicMock where attribute access returns _ColMock instances."""
    m = MagicMock()
    m.__class__ = type("ModelMock", (MagicMock,), {
        "__getattr__": lambda self, name: _ColMock(),
    })
    return m


# Patch airflow model stubs to use comparison-safe mocks
_models_stub = sys.modules["airflow.models"]
for _model_name in ["DagRun", "TaskInstance", "DagModel", "SlaMiss", "Pool"]:
    setattr(_models_stub, _model_name, _make_model_mock())


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_datetime():
    """Provide a sample datetime for tests."""
    return datetime(2024, 1, 15, 10, 30, 0)


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
                try_number=1,
                max_tries=3,
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
