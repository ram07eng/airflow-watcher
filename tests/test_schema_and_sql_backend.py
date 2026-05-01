"""Tests for SchemaConfig, SQLClient, factory routing, and ExternalBackend."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# SchemaConfig
# ---------------------------------------------------------------------------


class TestSchemaConfigDefaults:
    def test_defaults_match_dbt_schema(self):
        from airflow_watcher.backends.schema_config import SchemaConfig

        s = SchemaConfig()
        assert s.col_dag_id == "dag_id"
        assert s.col_results_array == "results"
        assert s.col_model_id == "unique_id"
        assert s.col_bytes_billed == "adapter_response.bytes_billed"
        assert s.structure == "nested_array"
        assert s.dialect == "bigquery"

    def test_from_env_no_override(self, monkeypatch):
        from airflow_watcher.backends.schema_config import SchemaConfig

        monkeypatch.delenv("AIRFLOW_WATCHER_SCHEMA_JSON", raising=False)
        s = SchemaConfig.from_env(table="p.d.t", dialect="bigquery")
        assert s.table == "p.d.t"
        assert s.col_dag_id == "dag_id"  # default unchanged

    def test_from_env_overrides_known_keys(self, monkeypatch):
        from airflow_watcher.backends.schema_config import SchemaConfig

        monkeypatch.setenv(
            "AIRFLOW_WATCHER_SCHEMA_JSON",
            json.dumps({"col_dag_id": "pipeline_id", "col_elapsed_time": "duration_sec"}),
        )
        s = SchemaConfig.from_env(table="t", dialect="bigquery")
        assert s.col_dag_id == "pipeline_id"
        assert s.col_elapsed_time == "duration_sec"
        assert s.col_task_id == "task_id"  # untouched default

    def test_from_env_unknown_key_warns(self, monkeypatch, caplog):
        import logging

        from airflow_watcher.backends.schema_config import SchemaConfig

        monkeypatch.setenv("AIRFLOW_WATCHER_SCHEMA_JSON", json.dumps({"no_such_field": "x"}))
        with caplog.at_level(logging.WARNING, logger="airflow_watcher.backends.schema_config"):
            SchemaConfig.from_env()
        assert "no_such_field" in caplog.text

    def test_from_env_invalid_json_raises(self, monkeypatch):
        from airflow_watcher.backends.schema_config import SchemaConfig

        monkeypatch.setenv("AIRFLOW_WATCHER_SCHEMA_JSON", "{not valid json}")
        with pytest.raises(ValueError, match="not valid JSON"):
            SchemaConfig.from_env()


class TestSchemaConfigFailureFilters:
    def test_failure_filter_nested_default(self):
        from airflow_watcher.backends.schema_config import SchemaConfig

        s = SchemaConfig()
        f = s.failure_filter_nested()
        assert "r.status = 'error'" in f
        assert "r.failures > 0" in f

    def test_failure_filter_flat_custom_columns(self):
        from airflow_watcher.backends.schema_config import SchemaConfig

        s = SchemaConfig(col_model_status_flat="run_state", col_model_failures_flat="err_count")
        f = s.failure_filter_flat()
        assert "run_state = 'error'" in f
        assert "err_count > 0" in f

    def test_failure_filter_no_count_guard(self):
        from airflow_watcher.backends.schema_config import SchemaConfig

        s = SchemaConfig(failure_count_guard="")
        f = s.failure_filter_nested()
        assert "failures" not in f
        assert "status = 'error'" in f

    def test_failure_filter_custom_status_value(self):
        from airflow_watcher.backends.schema_config import SchemaConfig

        s = SchemaConfig(failure_status_value="FAILED")
        f = s.failure_filter_flat()
        assert "= 'FAILED'" in f


# ---------------------------------------------------------------------------
# ExternalBackend / BigQueryBackend alias
# ---------------------------------------------------------------------------


class TestExternalBackendAlias:
    def test_bigquery_backend_is_external_backend(self):
        from airflow_watcher.backends.bigquery_backend import BigQueryBackend, ExternalBackend

        assert BigQueryBackend is ExternalBackend

    def test_backend_note_uses_label(self):
        from airflow_watcher.backends.bigquery_backend import ExternalBackend

        mock_client = MagicMock()
        be = ExternalBackend(client=mock_client, backend_label="mydb")
        result = be.get_sla_statistics()
        assert "mydb" in result["backend_note"]


# ---------------------------------------------------------------------------
# ExternalBackend — _Row
# ---------------------------------------------------------------------------


class TestRow:
    def test_to_dict_returns_self(self):
        from airflow_watcher.backends.bigquery_backend import _Row

        r = _Row({"dag_id": "d", "run_id": "r"})
        assert r.to_dict() is r


# ---------------------------------------------------------------------------
# Factory — get_backend
# ---------------------------------------------------------------------------


class TestGetBackend:
    def test_airflow_returns_none(self):
        from airflow_watcher.backends import get_backend

        cfg = MagicMock()
        cfg.backend = "airflow"
        assert get_backend(cfg) is None

    def test_unknown_returns_none(self):
        from airflow_watcher.backends import get_backend

        cfg = MagicMock()
        cfg.backend = "oracle"
        assert get_backend(cfg) is None

    def test_bigquery_missing_table_raises(self):
        from airflow_watcher.backends import get_backend

        cfg = MagicMock()
        cfg.backend = "bigquery"
        cfg.bq_table = None
        with pytest.raises(ValueError, match="AIRFLOW_WATCHER_BQ_TABLE"):
            get_backend(cfg)

    def test_sqlalchemy_missing_db_uri_raises(self, monkeypatch):
        from airflow_watcher.backends import get_backend

        monkeypatch.delenv("AIRFLOW_WATCHER_EXTERNAL_DB_URI", raising=False)
        cfg = MagicMock()
        cfg.backend = "sqlalchemy"
        cfg.bq_table = "schema.table"
        cfg.external_db_uri = None
        with pytest.raises(ValueError, match="AIRFLOW_WATCHER_EXTERNAL_DB_URI"):
            get_backend(cfg)

    def test_sqlalchemy_missing_table_raises(self, monkeypatch):
        from airflow_watcher.backends import get_backend

        monkeypatch.setenv("AIRFLOW_WATCHER_EXTERNAL_DB_URI", "sqlite:///:memory:")
        monkeypatch.delenv("AIRFLOW_WATCHER_BQ_TABLE", raising=False)
        cfg = MagicMock()
        cfg.backend = "sqlalchemy"
        cfg.bq_table = None
        cfg.external_db_uri = "sqlite:///:memory:"
        with pytest.raises(ValueError, match="AIRFLOW_WATCHER_BQ_TABLE"):
            get_backend(cfg)

    def test_bigquery_returns_external_backend(self, monkeypatch):
        from airflow_watcher.backends import get_backend
        from airflow_watcher.backends.bigquery_backend import ExternalBackend

        monkeypatch.delenv("AIRFLOW_WATCHER_SCHEMA_JSON", raising=False)
        cfg = MagicMock()
        cfg.backend = "bigquery"
        cfg.bq_table = "p.d.t"
        cfg.bq_structure = "nested_array"

        with patch("airflow_watcher.backends._bq_client.BQClient.__init__", return_value=None):
            result = get_backend(cfg)

        assert isinstance(result, ExternalBackend)

    def test_sqlalchemy_returns_external_backend(self, monkeypatch):
        from airflow_watcher.backends import get_backend
        from airflow_watcher.backends.bigquery_backend import ExternalBackend

        monkeypatch.delenv("AIRFLOW_WATCHER_SCHEMA_JSON", raising=False)
        cfg = MagicMock()
        cfg.backend = "sqlalchemy"
        cfg.bq_table = "my_table"
        cfg.external_db_uri = "sqlite:///:memory:"

        with patch("airflow_watcher.backends._sql_client.SQLClient.__init__", return_value=None):
            result = get_backend(cfg)

        assert isinstance(result, ExternalBackend)


# ---------------------------------------------------------------------------
# SQLClient — structure guard
# ---------------------------------------------------------------------------


class TestSQLClientStructureGuard:
    def test_raises_for_nested_array(self):
        from airflow_watcher.backends._sql_client import SQLClient
        from airflow_watcher.backends.schema_config import SchemaConfig

        s = SchemaConfig(structure="nested_array")
        with pytest.raises(ValueError, match="structure='flat'"):
            SQLClient(db_uri="sqlite:///:memory:", schema=s)


# ---------------------------------------------------------------------------
# SQLClient — live queries against SQLite in-memory DB
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_client(monkeypatch):
    """SQLClient wired to a SQLite in-memory DB with test data.

    The test conftest stubs ``sqlalchemy`` with a MagicMock.  We
    temporarily remove those stubs so that the real SQLAlchemy package
    is imported; monkeypatch restores them after the test.
    """
    import sys

    # Remove conftest sqlalchemy stubs so real package is importable.
    for key in list(sys.modules.keys()):
        if key == "sqlalchemy" or key.startswith("sqlalchemy."):
            monkeypatch.delitem(sys.modules, key, raising=False)

    from sqlalchemy import create_engine, text
    from sqlalchemy.pool import StaticPool

    from airflow_watcher.backends._sql_client import SQLClient
    from airflow_watcher.backends.schema_config import SchemaConfig

    schema = SchemaConfig(
        table="task_runs",
        dialect="sqlite",
        structure="flat",
        col_dag_id="dag_id",
        col_task_id="task_id",
        col_run_id="run_id",
        col_execution_date="execution_date",
        col_elapsed_time="elapsed_time",
        col_model_id_flat="model_id",
        col_model_status_flat="model_status",
        col_model_failures_flat="failure_count",
        col_model_message_flat="message",
        col_model_execution_time_flat="model_execution_time",
        col_bytes_billed_flat="bytes_billed",
        col_slot_ms_flat="slot_ms",
        col_state="model_status",
    )

    # StaticPool: every engine.connect() reuses the same connection,
    # so the in-memory DB is shared across all queries in the test.
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.connect() as conn:
        conn.execute(
            text("""
            CREATE TABLE task_runs (
                dag_id TEXT, task_id TEXT, run_id TEXT,
                execution_date TIMESTAMP,
                elapsed_time REAL,
                model_id TEXT, model_status TEXT, failure_count INTEGER,
                message TEXT, model_execution_time REAL,
                bytes_billed REAL, slot_ms REAL
            )
            """)
        )
        # Use naive UTC — consistent with SQLClient._cutoff() naive datetime
        now = datetime.utcnow()
        conn.execute(
            text("""
            INSERT INTO task_runs VALUES
                ('dag_a','t1','r1',:now, 120.0,'m1','success',0,'ok',10.0,1e9,500),
                ('dag_a','t2','r2',:now,  30.0,'m2','error',  1,'fail',5.0,NULL,NULL),
                ('dag_b','t3','r3',:now, 200.0,'m3','error',  2,'fail',20.0,2e9,800)
            """),
            {"now": now},
        )
        conn.commit()

    client = SQLClient.__new__(SQLClient)
    client._engine = engine
    client._s = schema
    client._text = text
    return client


class TestSQLClientLive:
    def test_get_recent_failures_returns_errors(self, sqlite_client):
        rows = sqlite_client.get_recent_failures(lookback_hours=1)
        dag_ids = [r["dag_id"] for r in rows]
        assert "dag_a" in dag_ids
        assert "dag_b" in dag_ids
        # success row excluded
        assert all(r["model_status"] != "success" for r in rows)

    def test_get_recent_failures_dag_filter(self, sqlite_client):
        rows = sqlite_client.get_recent_failures(lookback_hours=1, dag_id="dag_b")
        assert len(rows) == 1
        assert rows[0]["dag_id"] == "dag_b"

    def test_get_failure_statistics(self, sqlite_client):
        stats = sqlite_client.get_failure_statistics(lookback_hours=1)
        assert stats["total_task_runs"] == 3
        assert stats["task_runs_with_failures"] == 2
        assert len(stats["most_failing_dags"]) >= 1

    def test_get_long_running_tasks_threshold(self, sqlite_client):
        # threshold = 1 min (60 s) → 120 s and 200 s rows qualify
        rows = sqlite_client.get_long_running_tasks(threshold_minutes=1)
        assert len(rows) == 2
        elapsed = [r["elapsed_time"] for r in rows]
        assert 200.0 in elapsed
        assert 120.0 in elapsed

    def test_get_dag_overview(self, sqlite_client):
        overview = sqlite_client.get_dag_overview(lookback_hours=1)
        dag_ids = {r["dag_id"] for r in overview}
        assert "dag_a" in dag_ids
        assert "dag_b" in dag_ids

    def test_get_scheduling_lag_returns_note(self, sqlite_client):
        result = sqlite_client.get_scheduling_lag(lookback_hours=1)
        assert "backend_note" in result
        assert result["delayed_dags"] == []

    def test_get_failure_correlations_no_pairs(self, sqlite_client):
        # Only 1 run per DAG → no co-occurrence >= 2
        result = sqlite_client.get_failure_correlations(lookback_hours=1)
        assert result["correlations"] == []

    def test_get_cost_analysis(self, sqlite_client):
        summary = sqlite_client.get_cost_analysis(lookback_hours=1)
        # dag_a t2 has NULL bytes_billed, dag_b has 2e9
        assert "total_gb_billed" in summary


# ---------------------------------------------------------------------------
# StandaloneConfig — new backend fields
# ---------------------------------------------------------------------------


class TestStandaloneConfigBackendFields:
    def test_bigquery_backend_no_db_uri_needed(self, monkeypatch):
        from airflow_watcher.api.standalone_config import StandaloneConfig

        monkeypatch.setenv("AIRFLOW_WATCHER_BACKEND", "bigquery")
        monkeypatch.setenv("AIRFLOW_WATCHER_BQ_TABLE", "p.d.t")
        monkeypatch.delenv("AIRFLOW_WATCHER_DB_URI", raising=False)
        cfg = StandaloneConfig.from_env()
        assert cfg.backend == "bigquery"
        assert cfg.bq_table == "p.d.t"

    def test_sqlalchemy_backend_no_db_uri_needed(self, monkeypatch):
        from airflow_watcher.api.standalone_config import StandaloneConfig

        monkeypatch.setenv("AIRFLOW_WATCHER_BACKEND", "sqlalchemy")
        monkeypatch.setenv("AIRFLOW_WATCHER_BQ_TABLE", "my_table")
        monkeypatch.setenv("AIRFLOW_WATCHER_EXTERNAL_DB_URI", "sqlite:///:memory:")
        monkeypatch.delenv("AIRFLOW_WATCHER_DB_URI", raising=False)
        cfg = StandaloneConfig.from_env()
        assert cfg.backend == "sqlalchemy"
        assert cfg.external_db_uri == "sqlite:///:memory:"

    def test_bq_structure_field(self, monkeypatch):
        from airflow_watcher.api.standalone_config import StandaloneConfig

        monkeypatch.setenv("AIRFLOW_WATCHER_BACKEND", "bigquery")
        monkeypatch.setenv("AIRFLOW_WATCHER_BQ_TABLE", "p.d.t")
        monkeypatch.setenv("AIRFLOW_WATCHER_BQ_STRUCTURE", "flat")
        cfg = StandaloneConfig.from_env()
        assert cfg.bq_structure == "flat"

    def test_schema_json_field(self, monkeypatch):
        from airflow_watcher.api.standalone_config import StandaloneConfig

        monkeypatch.setenv("AIRFLOW_WATCHER_BACKEND", "bigquery")
        monkeypatch.setenv("AIRFLOW_WATCHER_BQ_TABLE", "p.d.t")
        monkeypatch.setenv("AIRFLOW_WATCHER_SCHEMA_JSON", '{"col_dag_id":"pipeline_id"}')
        cfg = StandaloneConfig.from_env()
        assert cfg.schema_json == '{"col_dag_id":"pipeline_id"}'
