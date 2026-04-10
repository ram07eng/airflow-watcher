"""Database session management for the standalone Airflow Watcher API.

Creates a SQLAlchemy engine from the configured DB URI and provides a
FastAPI dependency that yields scoped sessions for use by monitors.
"""

import logging
import sys
import warnings
from typing import Generator, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

logger = logging.getLogger(__name__)

_engine = None
_read_engine = None
_SessionLocal = None


def init_db(db_uri: str, query_timeout_ms: int = 30000, pool_size: int = 5, max_overflow: int = 10,
            db_read_uri: Optional[str] = None) -> None:
    """Initialise the SQLAlchemy engine(s) and test connectivity.

    Args:
        db_uri: SQLAlchemy database connection string (primary).
        query_timeout_ms: Per-statement timeout in milliseconds (PostgreSQL only).
        pool_size: Number of persistent connections in the pool.
        max_overflow: Max temporary connections above pool_size.
        db_read_uri: Optional read-replica connection string. When set,
            ``AIRFLOW__CORE__SQL_ALCHEMY_CONN`` is pointed at the replica so
            monitors' ``@provide_session`` queries run against it. The primary
            engine is still used for ``/healthz`` connectivity checks.

    Raises:
        SystemExit: If the database is unreachable.
    """
    global _engine, _read_engine, _SessionLocal

    def _make_engine(uri: str) -> "Engine":
        connect_args = {}
        if "postgresql" in uri:
            connect_args["options"] = f"-c statement_timeout={query_timeout_ms}"
        return create_engine(
            uri,
            pool_pre_ping=True,
            pool_size=pool_size,
            max_overflow=max_overflow,
            connect_args=connect_args,
        )

    _engine = _make_engine(db_uri)
    _SessionLocal = sessionmaker(bind=_engine, autocommit=False, autoflush=False)

    # Test primary connectivity
    try:
        with _engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        logger.error("Cannot connect to primary database: %s", exc)
        sys.exit(1)

    _log_connection("Primary database", db_uri)

    # Optional read replica
    if db_read_uri:
        _read_engine = _make_engine(db_read_uri)
        try:
            with _read_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as exc:
            logger.error("Cannot connect to read-replica database: %s", exc)
            sys.exit(1)
        _log_connection("Read-replica database", db_read_uri)

        # Route Airflow's @provide_session to the read replica.
        import os
        os.environ.setdefault("AIRFLOW__CORE__SQL_ALCHEMY_CONN", db_read_uri)
        logger.info("Monitors will query the read-replica database")
    else:
        _read_engine = None


def _log_connection(label: str, uri: str) -> None:
    """Log host/port without exposing credentials."""
    try:
        from urllib.parse import urlparse
        _parsed = urlparse(uri)
        logger.info(
            "%s connection established: %s://%s:%s",
            label, _parsed.scheme, _parsed.hostname, _parsed.port or "default",
        )
    except Exception:
        logger.info("%s connection established", label)


def get_engine():
    """Return the current SQLAlchemy engine instance.

    Returns:
        The SQLAlchemy Engine, or None if init_db() has not been called.
    """
    return _engine


def get_read_engine():
    """Return the read-replica engine, falling back to the primary engine.

    Returns:
        The read-replica Engine if configured, otherwise the primary Engine,
        or None if ``init_db()`` has not been called.
    """
    return _read_engine or _engine


def get_session_factory():
    """Return the session factory, or None if init_db() has not been called."""
    return _SessionLocal


def get_session() -> Generator[Session, None, None]:
    """FastAPI dependency that yields a scoped database session.

    .. deprecated::
        This function is unused in production code. Use ``get_session_factory()``
        instead and manage sessions directly.

    Yields:
        A SQLAlchemy Session that is automatically closed after the request.
    """
    warnings.warn(
        "get_session() is deprecated and unused. Use get_session_factory() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    if _SessionLocal is None:
        raise RuntimeError("Database not initialised. Call init_db() first.")

    session = _SessionLocal()
    try:
        yield session
    finally:
        session.close()
