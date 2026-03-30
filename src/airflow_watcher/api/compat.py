"""Compatibility shim so the standalone API can run without apache-airflow installed.

When Airflow is available (plugin mode), the real modules are used.
When Airflow is absent (standalone mode), lightweight stubs are injected into
``sys.modules`` so that ``from airflow.utils import timezone`` etc. work.

The model stubs created here carry **declared column attributes** that mirror
the real Airflow tables.  ``reflect_airflow_models()`` later maps them to
actual database tables via ``map_imperatively``.  This avoids fragile
``type("Foo", (), {})`` stubs that have no column metadata at all.
"""

import logging
import sys
from datetime import datetime
from datetime import timezone as _tz
from types import ModuleType

logger = logging.getLogger(__name__)


def _utcnow():
    return datetime.now(_tz.utc)


def _is_naive(dt):
    return dt.tzinfo is None


def _is_localized(dt):
    return dt.tzinfo is not None


is_naive = _is_naive
is_localized = _is_localized


def _make_aware(dt, tz=_tz.utc):
    if _is_naive(dt):
        return dt.replace(tzinfo=tz)
    return dt


def install_airflow_stubs():
    """Install minimal stubs into sys.modules if Airflow is not installed."""
    try:
        import airflow  # noqa: F401

        return  # Airflow is available, nothing to do
    except ImportError:
        pass

    # --- airflow (root) ---
    airflow_mod = ModuleType("airflow")
    airflow_mod.__path__ = []
    sys.modules["airflow"] = airflow_mod

    # --- airflow.utils ---
    airflow_utils = ModuleType("airflow.utils")
    airflow_utils.__path__ = []
    sys.modules["airflow.utils"] = airflow_utils

    # --- airflow.utils.timezone ---
    tz_mod = ModuleType("airflow.utils.timezone")
    tz_mod.utcnow = _utcnow
    tz_mod.is_naive = _is_naive
    tz_mod.is_localized = _is_localized
    tz_mod.make_aware = _make_aware
    sys.modules["airflow.utils.timezone"] = tz_mod

    # --- airflow.utils.state ---
    state_mod = ModuleType("airflow.utils.state")

    class _DagRunState:
        SUCCESS = "success"
        FAILED = "failed"
        RUNNING = "running"
        QUEUED = "queued"

    class _TaskInstanceState:
        SUCCESS = "success"
        FAILED = "failed"
        RUNNING = "running"
        UPSTREAM_FAILED = "upstream_failed"
        SKIPPED = "skipped"
        UP_FOR_RETRY = "up_for_retry"
        UP_FOR_RESCHEDULE = "up_for_reschedule"
        QUEUED = "queued"
        SCHEDULED = "scheduled"
        DEFERRED = "deferred"
        REMOVED = "removed"

    state_mod.DagRunState = _DagRunState
    state_mod.TaskInstanceState = _TaskInstanceState
    sys.modules["airflow.utils.state"] = state_mod

    # --- airflow.utils.session (provide_session creates a session from our engine) ---
    session_mod = ModuleType("airflow.utils.session")

    def _provide_session(func):
        """Decorator that injects a SQLAlchemy session from the standalone DB engine."""
        import functools

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if kwargs.get("session") is None:
                from airflow_watcher.api.db import get_session_factory

                factory = get_session_factory()
                if factory is not None:
                    session = factory()
                    try:
                        kwargs["session"] = session
                        result = func(*args, **kwargs)
                        session.commit()
                        return result
                    except Exception:
                        session.rollback()
                        raise
                    finally:
                        session.close()
            return func(*args, **kwargs)

        return wrapper

    session_mod.provide_session = _provide_session
    sys.modules["airflow.utils.session"] = session_mod

    # --- airflow.utils.db ---
    db_mod = ModuleType("airflow.utils.db")
    db_mod.provide_session = _provide_session
    sys.modules["airflow.utils.db"] = db_mod

    # --- airflow.models ---
    models_mod = ModuleType("airflow.models")
    models_mod.__path__ = []

    # Proper stub classes with column metadata so monitors can reference
    # attributes before ``reflect_airflow_models()`` maps them to real tables.
    # ``map_imperatively()`` later binds these classes to reflected tables.

    class _StubBase:
        """Marker base – not a real DeclarativeBase, just provides __init__."""

        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    class _DagRun(_StubBase):
        dag_id = None
        run_id = None
        state = None
        execution_date = None
        start_date = None
        end_date = None
        run_type = None
        conf = None
        external_trigger = None

    class _DagModel(_StubBase):
        dag_id = None
        is_paused = None
        is_active = None
        fileloc = None
        owners = None
        last_parsed_time = None
        schedule_interval = None
        timetable_description = None

    class _TaskInstance(_StubBase):
        dag_id = None
        task_id = None
        run_id = None
        state = None
        start_date = None
        end_date = None
        duration = None
        try_number = None
        max_tries = None
        hostname = None
        executor_config = None
        pool = None
        queue = None
        operator = None
        execution_date = None

    class _SlaMiss(_StubBase):
        dag_id = None
        task_id = None
        execution_date = None
        timestamp = None
        description = None
        notification_sent = None
        email_sent = None

    class _Pool(_StubBase):
        pool = None
        slots = None
        description = None

    class _ImportError(_StubBase):
        filename = None
        stacktrace = None
        timestamp = None

    class _DagBag:
        """Minimal DagBag stub – dependency analysis is not supported standalone."""

        def __init__(self, *args, **kwargs):
            self.dags = {}

    class _DagTag(_StubBase):
        dag_id = None
        name = None

    models_mod.DagRun = _DagRun
    models_mod.DagModel = _DagModel
    models_mod.TaskInstance = _TaskInstance
    models_mod.SlaMiss = _SlaMiss
    models_mod.Pool = _Pool
    models_mod.ImportError = _ImportError
    models_mod.DagBag = _DagBag
    models_mod.DagTag = _DagTag
    sys.modules["airflow.models"] = models_mod

    # --- airflow.models.serialized_dag ---
    sd_mod = ModuleType("airflow.models.serialized_dag")
    sd_mod.SerializedDagModel = type("SerializedDagModel", (), {})
    sys.modules["airflow.models.serialized_dag"] = sd_mod

    # --- airflow.configuration ---
    conf_mod = ModuleType("airflow.configuration")
    conf_mod.conf = type(
        "conf",
        (),
        {
            "get": staticmethod(lambda *a, **kw: ""),
            "getint": staticmethod(lambda *a, **kw: 0),
            "getboolean": staticmethod(lambda *a, **kw: False),
        },
    )()
    sys.modules["airflow.configuration"] = conf_mod

    # --- airflow.plugins_manager ---
    plugins_mod = ModuleType("airflow.plugins_manager")
    plugins_mod.AirflowPlugin = type("AirflowPlugin", (), {"name": ""})
    sys.modules["airflow.plugins_manager"] = plugins_mod

    # --- airflow.security ---
    security_mod = ModuleType("airflow.security")
    security_mod.__path__ = []
    sys.modules["airflow.security"] = security_mod

    perms_mod = ModuleType("airflow.security.permissions")
    perms_mod.ACTION_CAN_READ = "can_read"
    perms_mod.RESOURCE_DAG = "DAG"
    sys.modules["airflow.security.permissions"] = perms_mod


def reflect_airflow_models(engine):
    """Reflect Airflow's metadata tables and patch the stub model classes in-place.

    Must be called after ``init_db()`` and ``install_airflow_stubs()``.
    Patches the stub classes (already referenced by monitors) so that
    ``session.query(DagRun)`` works against real Airflow tables.
    """
    from sqlalchemy import MetaData, Table
    from sqlalchemy.exc import NoSuchTableError, OperationalError
    from sqlalchemy.orm import registry

    metadata = MetaData()
    mapper_registry = registry()
    models = sys.modules.get("airflow.models")
    sd_mod = sys.modules.get("airflow.models.serialized_dag")

    _TABLE_MAP = {
        "DagRun": "dag_run",
        "DagModel": "dag",
        "TaskInstance": "task_instance",
        "SlaMiss": "sla_miss",
        "Pool": "slot_pool",
        "ImportError": "import_error",
        "DagTag": "dag_tag",
    }

    for attr, table_name in _TABLE_MAP.items():
        try:
            table = Table(table_name, metadata, autoload_with=engine)
            cls = getattr(models, attr)
            mapper_registry.map_imperatively(cls, table)
        except (NoSuchTableError, OperationalError) as exc:
            logger.warning("Table %r not found in DB, skipping %s mapping: %s", table_name, attr, exc)
        except Exception as exc:
            logger.debug("Skipped mapping %s: %s", attr, exc)

    # serialized_dag table
    try:
        sd_table = Table("serialized_dag", metadata, autoload_with=engine)
        mapper_registry.map_imperatively(sd_mod.SerializedDagModel, sd_table)
    except (NoSuchTableError, OperationalError) as exc:
        logger.warning("serialized_dag table not found: %s", exc)
    except Exception as exc:
        logger.debug("Skipped mapping SerializedDagModel: %s", exc)
