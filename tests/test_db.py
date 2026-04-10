"""Tests for api/db.py — SQLAlchemy engine and session management."""

from unittest.mock import MagicMock, patch

import pytest

from airflow_watcher.api import db


@pytest.fixture(autouse=True)
def reset_db_globals():
    """Reset module-level globals before each test."""
    db._engine = None
    db._read_engine = None
    db._SessionLocal = None
    yield
    db._engine = None
    db._read_engine = None
    db._SessionLocal = None


class TestInitDb:
    """Tests for init_db()."""

    @patch("airflow_watcher.api.db.create_engine")
    @patch("airflow_watcher.api.db.sessionmaker")
    def test_creates_engine_and_session_factory(self, mock_sessionmaker, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        db.init_db("sqlite:///test.db")

        mock_create_engine.assert_called_once_with(
            "sqlite:///test.db", pool_pre_ping=True, pool_size=5, max_overflow=10, connect_args={}
        )
        mock_sessionmaker.assert_called_once_with(bind=mock_engine, autocommit=False, autoflush=False)
        assert db._engine is mock_engine

    @patch("airflow_watcher.api.db.create_engine")
    def test_exits_on_unreachable_db(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.side_effect = Exception("Connection refused")

        with pytest.raises(SystemExit) as exc_info:
            db.init_db("postgresql://user:pass@badhost/db")
        assert exc_info.value.code == 1

    @patch("airflow_watcher.api.db.create_engine")
    @patch("airflow_watcher.api.db.sessionmaker")
    def test_logs_error_on_unreachable_db(self, mock_sessionmaker, mock_create_engine, caplog):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.side_effect = Exception("Connection refused")

        import logging

        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit):
                db.init_db("postgresql://user:pass@badhost/db")

        assert "Cannot connect to primary database" in caplog.text
        assert "Connection refused" in caplog.text


class TestGetEngine:
    """Tests for get_engine()."""

    def test_returns_none_before_init(self):
        assert db.get_engine() is None

    @patch("airflow_watcher.api.db.create_engine")
    @patch("airflow_watcher.api.db.sessionmaker")
    def test_returns_engine_after_init(self, mock_sessionmaker, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        db.init_db("sqlite:///test.db")
        assert db.get_engine() is mock_engine


class TestGetSession:
    """Tests for get_session() FastAPI dependency."""

    def test_raises_if_not_initialised(self):
        gen = db.get_session()
        with pytest.raises(RuntimeError, match="Database not initialised"):
            next(gen)

    def test_yields_session_and_closes(self):
        mock_session = MagicMock()
        mock_factory = MagicMock(return_value=mock_session)
        db._SessionLocal = mock_factory

        gen = db.get_session()
        session = next(gen)

        assert session is mock_session

        # Exhaust the generator to trigger finally block
        with pytest.raises(StopIteration):
            next(gen)

        mock_session.close.assert_called_once()

    def test_closes_session_on_exception(self):
        mock_session = MagicMock()
        mock_factory = MagicMock(return_value=mock_session)
        db._SessionLocal = mock_factory

        gen = db.get_session()
        next(gen)

        # Simulate an exception during request handling
        with pytest.raises(ValueError):
            gen.throw(ValueError("request error"))

        mock_session.close.assert_called_once()


class TestGetReadEngine:
    """Tests for get_read_engine()."""

    def test_returns_none_before_init(self):
        assert db.get_read_engine() is None

    @patch("airflow_watcher.api.db.create_engine")
    @patch("airflow_watcher.api.db.sessionmaker")
    def test_falls_back_to_primary_when_no_replica(self, mock_sessionmaker, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        db.init_db("sqlite:///test.db")
        assert db.get_read_engine() is mock_engine

    @patch("airflow_watcher.api.db.create_engine")
    @patch("airflow_watcher.api.db.sessionmaker")
    def test_returns_read_engine_when_configured(self, mock_sessionmaker, mock_create_engine):
        primary_engine = MagicMock(name="primary")
        read_engine = MagicMock(name="read")
        mock_create_engine.side_effect = [primary_engine, read_engine]
        for eng in (primary_engine, read_engine):
            mock_conn = MagicMock()
            eng.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
            eng.connect.return_value.__exit__ = MagicMock(return_value=False)

        db.init_db("sqlite:///primary.db", db_read_uri="sqlite:///replica.db")
        assert db.get_read_engine() is read_engine
        assert db.get_engine() is primary_engine


class TestReadReplica:
    """Tests for read-replica engine creation."""

    @patch("airflow_watcher.api.db.create_engine")
    @patch("airflow_watcher.api.db.sessionmaker")
    def test_creates_two_engines_with_read_uri(self, mock_sessionmaker, mock_create_engine):
        primary_engine = MagicMock(name="primary")
        read_engine = MagicMock(name="read")
        mock_create_engine.side_effect = [primary_engine, read_engine]
        for eng in (primary_engine, read_engine):
            mock_conn = MagicMock()
            eng.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
            eng.connect.return_value.__exit__ = MagicMock(return_value=False)

        db.init_db("sqlite:///primary.db", db_read_uri="sqlite:///replica.db")

        assert mock_create_engine.call_count == 2
        assert db._engine is primary_engine
        assert db._read_engine is read_engine

    @patch("airflow_watcher.api.db.create_engine")
    @patch("airflow_watcher.api.db.sessionmaker")
    def test_read_replica_sets_airflow_env_var(self, mock_sessionmaker, mock_create_engine, monkeypatch):
        monkeypatch.delenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN", raising=False)
        primary_engine = MagicMock(name="primary")
        read_engine = MagicMock(name="read")
        mock_create_engine.side_effect = [primary_engine, read_engine]
        for eng in (primary_engine, read_engine):
            mock_conn = MagicMock()
            eng.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
            eng.connect.return_value.__exit__ = MagicMock(return_value=False)

        import os

        db.init_db("sqlite:///primary.db", db_read_uri="sqlite:///replica.db")
        assert os.environ.get("AIRFLOW__CORE__SQL_ALCHEMY_CONN") == "sqlite:///replica.db"

        # Clean up
        monkeypatch.delenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN", raising=False)

    @patch("airflow_watcher.api.db.create_engine")
    def test_exits_on_unreachable_read_replica(self, mock_create_engine):
        primary_engine = MagicMock(name="primary")
        read_engine = MagicMock(name="read")
        mock_create_engine.side_effect = [primary_engine, read_engine]

        # Primary succeeds
        primary_conn = MagicMock()
        primary_engine.connect.return_value.__enter__ = MagicMock(return_value=primary_conn)
        primary_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        # Read replica fails
        read_engine.connect.side_effect = Exception("Replica unreachable")

        with pytest.raises(SystemExit) as exc_info:
            db.init_db("sqlite:///primary.db", db_read_uri="sqlite:///replica.db")
        assert exc_info.value.code == 1

    @patch("airflow_watcher.api.db.create_engine")
    @patch("airflow_watcher.api.db.sessionmaker")
    def test_no_read_engine_without_read_uri(self, mock_sessionmaker, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        db.init_db("sqlite:///test.db")
        assert db._read_engine is None
