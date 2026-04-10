"""Tests for api/main.py — FastAPI entry point, /healthz, and X-API-Version middleware."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from airflow_watcher.api import main as main_module
from airflow_watcher.api.main import create_app

_SENTINEL = object()


def _make_app(get_engine_return=_SENTINEL, engine_connect_side_effect=None):
    """Helper to create a test app with mocked dependencies.

    Returns (app, config, mock_init_db, mock_get_engine, patches).
    Pass ``get_engine_return=None`` to simulate a missing engine.
    """
    mock_config_cls = patch("airflow_watcher.api.main.StandaloneConfig")
    mock_init_db_p = patch("airflow_watcher.api.main.init_db")
    mock_get_engine_p = patch("airflow_watcher.api.main.get_engine")
    mock_get_read_engine_p = patch("airflow_watcher.api.main.get_read_engine")
    mock_dotenv_p = patch("airflow_watcher.api.main.load_dotenv")

    mc = mock_config_cls.start()
    mi = mock_init_db_p.start()
    me = mock_get_engine_p.start()
    mre = mock_get_read_engine_p.start()
    mock_dotenv_p.start()

    cfg = MagicMock()
    cfg.db_uri = "sqlite:///test.db"
    cfg.api_host = "0.0.0.0"
    cfg.api_port = 8081
    cfg.rate_limit_rpm = 120
    cfg.query_timeout_ms = 30000
    cfg.request_timeout_seconds = 60
    cfg.log_format = "text"
    cfg.log_level = "INFO"
    cfg.rbac_fail_open = True
    mc.from_env.return_value = cfg

    if get_engine_return is not _SENTINEL:
        me.return_value = get_engine_return
        # When engine is None or specific, read engine returns the same
        mre.return_value = get_engine_return
    elif engine_connect_side_effect is not None:
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = engine_connect_side_effect
        me.return_value = mock_engine
        mre.return_value = mock_engine
    else:
        # Default: healthy DB engine
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        me.return_value = mock_engine
        # No read replica by default: same as primary (no separate field shown)
        mre.return_value = mock_engine

    app, config = create_app()
    return app, config, mi, me, [mock_config_cls, mock_init_db_p, mock_get_engine_p, mock_get_read_engine_p, mock_dotenv_p]


@pytest.fixture
def app_and_config():
    """Create a test app with mocked config and DB."""
    app, config, mock_init_db, mock_get_engine, patches = _make_app()
    yield app, config, mock_init_db, mock_get_engine
    for p in patches:
        p.stop()


@pytest.fixture
def client(app_and_config):
    """Provide a TestClient for the app."""
    app, *_ = app_and_config
    return TestClient(app)


class TestCreateApp:
    """Tests for create_app()."""

    def test_returns_app_and_config(self, app_and_config):
        app, config, mock_init_db, _ = app_and_config
        assert app is not None
        assert config is not None
        assert config.db_uri == "sqlite:///test.db"

    def test_calls_init_db_with_config_uri(self, app_and_config):
        _, config, mock_init_db, _ = app_and_config
        mock_init_db.assert_called_once_with(
            config.db_uri, query_timeout_ms=config.query_timeout_ms,
            pool_size=config.db_pool_size, max_overflow=config.db_max_overflow,
            db_read_uri=config.db_read_uri
        )

    def test_records_start_time(self, app_and_config):
        assert main_module._start_time > 0


class TestHealthzEndpoint:
    """Tests for GET /healthz."""

    def test_returns_200(self, client):
        resp = client.get("/healthz")
        assert resp.status_code == 200

    def test_response_contains_required_fields(self, client):
        resp = client.get("/healthz")
        body = resp.json()
        assert "status" in body
        assert "uptime_seconds" in body
        assert "db_connected" in body

    def test_status_ok_when_db_connected(self, client):
        resp = client.get("/healthz")
        body = resp.json()
        assert body["status"] == "ok"
        assert body["db_connected"] is True

    def test_uptime_is_non_negative(self, client):
        resp = client.get("/healthz")
        body = resp.json()
        assert body["uptime_seconds"] >= 0

    def test_status_degraded_when_db_unreachable(self):
        app, _, _, _, patches = _make_app(
            engine_connect_side_effect=Exception("Connection refused"),
        )
        try:
            c = TestClient(app)
            resp = c.get("/healthz")
            body = resp.json()
            assert body["status"] == "degraded"
            assert body["db_connected"] is False
        finally:
            for p in patches:
                p.stop()

    def test_status_degraded_when_engine_is_none(self):
        app, _, _, _, patches = _make_app(get_engine_return=None)
        try:
            c = TestClient(app)
            resp = c.get("/healthz")
            body = resp.json()
            assert body["status"] == "degraded"
            assert body["db_connected"] is False
        finally:
            for p in patches:
                p.stop()


class TestApiVersionMiddleware:
    """Tests for X-API-Version: 1.0 response header."""

    def test_healthz_has_version_header(self, client):
        resp = client.get("/healthz")
        assert resp.headers.get("x-api-version") == "1.0"

    def test_unknown_route_has_version_header(self, client):
        resp = client.get("/nonexistent")
        assert resp.headers.get("x-api-version") == "1.0"
