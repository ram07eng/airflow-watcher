"""Tests for api/rbac_dep.py — RBAC dependency."""

import pytest
from fastapi import Depends, FastAPI, HTTPException
from fastapi.testclient import TestClient

from airflow_watcher.api.auth import configure_auth
from airflow_watcher.api.rbac_dep import (
    check_dag_access,
    configure_rbac,
    filter_dags,
    get_allowed_dag_ids,
)


class TestConfigureRbac:
    def test_returns_none_when_disabled(self):
        configure_rbac(False, {})
        configure_auth([])

        app = FastAPI()

        @app.get("/test")
        async def endpoint(allowed=Depends(get_allowed_dag_ids)):
            return {"allowed": list(allowed) if allowed else None}

        client = TestClient(app)
        resp = client.get("/test")
        assert resp.status_code == 200
        assert resp.json()["allowed"] is None

    def test_returns_dag_set_when_enabled(self):
        configure_rbac(True, {"my-key": ["dag_a", "dag_b"]})
        configure_auth(["my-key"])

        app = FastAPI()

        @app.get("/test")
        async def endpoint(allowed=Depends(get_allowed_dag_ids)):
            return {"allowed": sorted(list(allowed)) if allowed else None}

        client = TestClient(app)
        resp = client.get("/test", headers={"Authorization": "Bearer my-key"})
        assert resp.status_code == 200
        assert resp.json()["allowed"] == ["dag_a", "dag_b"]

    def test_unknown_key_returns_empty_set(self):
        configure_rbac(True, {"known": ["dag_x"]})
        configure_auth(["unknown-key"])

        app = FastAPI()

        @app.get("/test")
        async def endpoint(allowed=Depends(get_allowed_dag_ids)):
            return {"allowed": list(allowed) if allowed is not None else None}

        client = TestClient(app)
        resp = client.get("/test", headers={"Authorization": "Bearer unknown-key"})
        assert resp.status_code == 200
        assert resp.json()["allowed"] == []


class TestCheckDagAccess:
    def test_no_restriction_passes(self):
        check_dag_access("any_dag", None)  # Should not raise

    def test_allowed_dag_passes(self):
        check_dag_access("dag_a", {"dag_a", "dag_b"})

    def test_denied_dag_raises_403(self):
        with pytest.raises(HTTPException) as exc_info:
            check_dag_access("dag_c", {"dag_a", "dag_b"})
        assert exc_info.value.status_code == 403


class TestFilterDags:
    def test_none_allowed_returns_all(self):
        items = [{"dag_id": "a"}, {"dag_id": "b"}]
        assert filter_dags(items, None) == items

    def test_filters_by_dag_id(self):
        items = [{"dag_id": "a"}, {"dag_id": "b"}, {"dag_id": "c"}]
        result = filter_dags(items, {"a", "c"})
        assert len(result) == 2
        assert result[0]["dag_id"] == "a"
        assert result[1]["dag_id"] == "c"

    def test_empty_allowed_returns_empty(self):
        items = [{"dag_id": "a"}]
        assert filter_dags(items, set()) == []
