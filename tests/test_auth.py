"""Tests for api/auth.py — Bearer token authentication."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from airflow_watcher.api.auth import configure_auth, require_auth


def _make_app_with_auth(api_keys):
    """Create a minimal FastAPI app with a protected endpoint."""
    configure_auth(api_keys)
    app = FastAPI()

    @app.get("/protected")
    async def protected(key=None):
        key = await require_auth(type("R", (), {"headers": {}})())  # unused
        return {"key": key}

    # Wire up properly via dependency
    from fastapi import Depends

    @app.get("/test")
    async def test_endpoint(token: str = Depends(require_auth)):
        return {"token": token}

    return app


class TestAuthEnabled:
    """Auth with configured keys."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.app = _make_app_with_auth(["valid-key-1", "valid-key-2"])
        self.client = TestClient(self.app)

    def test_valid_key_returns_200(self):
        resp = self.client.get("/test", headers={"Authorization": "Bearer valid-key-1"})
        assert resp.status_code == 200
        assert resp.json()["token"] == "valid-key-1"

    def test_second_valid_key(self):
        resp = self.client.get("/test", headers={"Authorization": "Bearer valid-key-2"})
        assert resp.status_code == 200

    def test_missing_header_returns_401(self):
        resp = self.client.get("/test")
        assert resp.status_code == 401

    def test_invalid_token_returns_401(self):
        resp = self.client.get("/test", headers={"Authorization": "Bearer bad-key"})
        assert resp.status_code == 401

    def test_wrong_scheme_returns_401(self):
        resp = self.client.get("/test", headers={"Authorization": "Basic abc"})
        assert resp.status_code == 401

    def test_error_body_contains_message(self):
        resp = self.client.get("/test")
        body = resp.json()["detail"]
        assert body["status"] == "error"
        assert body["message"] == "Authentication required"


class TestAuthDisabled:
    """Auth when api_keys is empty (disabled)."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.app = _make_app_with_auth([])
        self.client = TestClient(self.app)

    def test_no_header_passes(self):
        resp = self.client.get("/test")
        assert resp.status_code == 200
        assert resp.json()["token"] is None

    def test_any_header_passes(self):
        resp = self.client.get("/test", headers={"Authorization": "Bearer anything"})
        assert resp.status_code == 200
