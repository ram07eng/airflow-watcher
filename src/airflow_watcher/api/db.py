"""Database session management for the standalone Airflow Watcher API.

Creates a SQLAlchemy engine from the configured DB URI and provides a
FastAPI dependency that yields scoped sessions for use by monitors.
"""

import logging
import sys
import warnings
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

logger = logging.getLogger(__name__)

_engine = None
_SessionLocal = None


def init_db(db_uri: str, query_timeout_ms: int = 30000) -> None:
    """Initialise the SQLAlchemy engine and test connectivity.

    Args:
        db_uri: SQLAlchemy database connection string.
        query_timeout_ms: Per-statement timeout in milliseconds (PostgreSQL only).

    Raises:
        SystemExit: If the database is unreachable.
    """
    global _engine, _SessionLocal

    connect_args = {}
    if "postgresql" in db_uri:
        connect_args["options"] = f"-c statement_timeout={query_timeout_ms}"

    _engine = create_engine(db_uri, pool_pre_ping=True, connect_args=connect_args)
    _SessionLocal = sessionmaker(bind=_engine, autocommit=False, autoflush=False)

    # Test connectivity
    try:
        with _engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        logger.error("Cannot connect to database: %s", exc)
        sys.exit(1)

    # Only log host/port, never credentials or full URI.
    try:
        from urllib.parse import urlparse

        _parsed = urlparse(db_uri)
        logger.info(
            "Database connection established: %s://%s:%s", _parsed.scheme, _parsed.hostname, _parsed.port or "default"
        )
    except Exception:
        logger.info("Database connection established")


def get_engine():
    """Return the current SQLAlchemy engine instance.

    Returns:
        The SQLAlchemy Engine, or None if init_db() has not been called.
    """
    return _engine


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
