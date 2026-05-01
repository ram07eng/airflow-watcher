"""Tests for BigQueryBackend and backend factory."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_backend(table="proj.ds.tbl"):
    """Build an ExternalBackend with a mocked client."""
    mock_client = MagicMock()
    from airflow_watcher.backends.bigquery_backend import ExternalBackend

    be = ExternalBackend.__new__(ExternalBackend)
    be._client = mock_client
    be._label = "bigquery"
    return be, mock_client


# ---------------------------------------------------------------------------
# _Row
# ---------------------------------------------------------------------------


class TestRow:
    def test_to_dict_returns_self(self):
        from airflow_watcher.backends.bigquery_backend import _Row

        r = _Row({"dag_id": "foo", "run_id": "bar"})
        assert r.to_dict() is r
        assert r["dag_id"] == "foo"


# ---------------------------------------------------------------------------
# Backend factory
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
        cfg.backend = "mysql"
        assert get_backend(cfg) is None

    def test_bigquery_missing_table_raises(self):
        from airflow_watcher.backends import get_backend

        cfg = MagicMock()
        cfg.backend = "bigquery"
        cfg.bq_table = None
        with pytest.raises(ValueError, match="AIRFLOW_WATCHER_BQ_TABLE"):
            get_backend(cfg)

    def test_bigquery_missing_package_raises(self):
        from airflow_watcher.backends import get_backend

        cfg = MagicMock()
        cfg.backend = "bigquery"
        cfg.bq_table = "proj.ds.tbl"

        with patch.dict("sys.modules", {"google.cloud.bigquery": None}):
            with patch(
                "airflow_watcher.backends.bigquery_backend.BigQueryBackend.__init__",
                side_effect=ImportError("google-cloud-bigquery"),
            ):
                with pytest.raises(ImportError, match="google-cloud-bigquery"):
                    get_backend(cfg)

    def test_bigquery_returns_instance(self, monkeypatch):
        from airflow_watcher.backends import get_backend
        from airflow_watcher.backends.bigquery_backend import ExternalBackend

        monkeypatch.delenv("AIRFLOW_WATCHER_SCHEMA_JSON", raising=False)
        cfg = MagicMock()
        cfg.backend = "bigquery"
        cfg.bq_table = "proj.ds.tbl"
        cfg.bq_structure = "nested_array"

        with patch("airflow_watcher.backends._bq_client.BQClient.__init__", return_value=None):
            result = get_backend(cfg)

        assert isinstance(result, ExternalBackend)


# ---------------------------------------------------------------------------
# BigQueryBackend — failure monitor interface
# ---------------------------------------------------------------------------


class TestBigQueryBackendFailures:
    def test_get_recent_failures_wraps_rows(self):
        from airflow_watcher.backends.bigquery_backend import _Row

        be, mock_bq = _make_backend()
        mock_bq.get_recent_failures.return_value = [
            {"dag_id": "a", "run_id": "r1"},
            {"dag_id": "b", "run_id": "r2"},
        ]
        rows = be.get_recent_failures(dag_id=None, lookback_hours=24, limit=50, offset=0)
        assert len(rows) == 2
        assert all(isinstance(r, _Row) for r in rows)
        assert rows[0].to_dict() is rows[0]

    def test_get_failure_statistics_delegates(self):
        be, mock_bq = _make_backend()
        mock_bq.get_failure_statistics.return_value = {"total_task_runs": 10}
        result = be.get_failure_statistics(lookback_hours=24)
        assert result["total_task_runs"] == 10
        mock_bq.get_failure_statistics.assert_called_once_with(lookback_hours=24)


# ---------------------------------------------------------------------------
# BigQueryBackend — SLA monitor interface
# ---------------------------------------------------------------------------


class TestBigQueryBackendSLA:
    def test_get_recent_sla_misses_empty(self):
        be, _ = _make_backend()
        result = be.get_recent_sla_misses()
        assert result == []

    def test_get_sla_statistics_has_backend_note(self):
        be, _ = _make_backend()
        result = be.get_sla_statistics()
        assert result["total_misses"] == 0
        assert "backend_note" in result


# ---------------------------------------------------------------------------
# BigQueryBackend — task monitor interface
# ---------------------------------------------------------------------------


class TestBigQueryBackendTasks:
    def test_get_long_running_tasks_delegates(self):
        be, mock_bq = _make_backend()
        mock_bq.get_long_running_tasks.return_value = [{"dag_id": "x"}]
        result = be.get_long_running_tasks(threshold_minutes=30)
        assert result == [{"dag_id": "x"}]

    def test_get_retry_heavy_tasks_empty(self):
        be, _ = _make_backend()
        assert be.get_retry_heavy_tasks() == []

    def test_get_zombie_tasks_empty(self):
        be, _ = _make_backend()
        assert be.get_zombie_tasks() == []

    def test_get_task_failure_patterns_structure(self):
        be, mock_bq = _make_backend()
        mock_bq.get_failure_statistics.return_value = {
            "most_failing_dags": [
                {"dag_id": "dag_a", "failure_count": 5, "total_runs": 10, "failure_rate": 0.5},
                {"dag_id": "dag_b", "failure_count": 9, "total_runs": 10, "failure_rate": 0.9},
            ]
        }
        result = be.get_task_failure_patterns(lookback_hours=48)
        assert len(result["top_failing_tasks"]) == 2
        # dag_b has rate 0.9 >= 0.8 → consistently failing
        assert len(result["consistently_failing_tasks"]) == 1
        assert result["consistently_failing_tasks"][0]["dag_id"] == "dag_b"
        assert "backend_note" in result


# ---------------------------------------------------------------------------
# BigQueryBackend — scheduling monitor interface
# ---------------------------------------------------------------------------


class TestBigQueryBackendScheduling:
    def test_get_scheduling_lag_delegates(self):
        be, mock_bq = _make_backend()
        mock_bq.get_scheduling_lag.return_value = {"p50_lag_minutes": 1.2}
        result = be.get_scheduling_lag(lookback_hours=24, lag_threshold_minutes=10)
        assert result["p50_lag_minutes"] == 1.2

    def test_get_queued_tasks_has_backend_note(self):
        be, _ = _make_backend()
        result = be.get_queued_tasks()
        assert result["queued_count"] == 0
        assert "backend_note" in result

    def test_get_pool_utilization_empty(self):
        be, _ = _make_backend()
        assert be.get_pool_utilization() == []

    def test_get_concurrent_runs_has_backend_note(self):
        be, _ = _make_backend()
        result = be.get_concurrent_runs()
        assert "backend_note" in result

    def test_get_stale_dags_filters_old(self):
        from datetime import datetime, timezone

        be, mock_bq = _make_backend()
        old_ts = datetime(2000, 1, 1, tzinfo=timezone.utc)
        recent_ts = datetime.now(tz=timezone.utc)
        mock_bq.get_dag_overview.return_value = [
            {"dag_id": "old_dag", "latest_execution": old_ts},
            {"dag_id": "fresh_dag", "latest_execution": recent_ts},
        ]
        stale = be.get_stale_dags(expected_interval_hours=24)
        assert len(stale) == 1
        assert stale[0]["dag_id"] == "old_dag"


# ---------------------------------------------------------------------------
# BigQueryBackend — DAG health monitor interface
# ---------------------------------------------------------------------------


class TestBigQueryBackendDagHealth:
    def test_get_dag_import_errors_empty(self):
        be, _ = _make_backend()
        assert be.get_dag_import_errors() == []

    def test_get_dag_status_summary_health_score(self):
        be, mock_bq = _make_backend()
        mock_bq.get_dag_overview.return_value = [
            {"dag_id": "d1", "failed_models": 0},
            {"dag_id": "d2", "failed_models": 2},
        ]
        result = be.get_dag_status_summary()
        assert result["total_dags"] == 2
        assert result["dags_with_failures"] == 1
        assert result["health_score"] == 50.0

    def test_get_dag_complexity_analysis_maps_unique_tasks(self):
        be, mock_bq = _make_backend()
        mock_bq.get_dag_overview.return_value = [
            {"dag_id": "d1", "unique_tasks": 7, "total_runs": 3},
        ]
        result = be.get_dag_complexity_analysis()
        assert result[0]["dag_id"] == "d1"
        assert result[0]["task_count"] == 7

    def test_get_inactive_dags_delegates_to_stale(self):
        from datetime import datetime, timezone

        be, mock_bq = _make_backend()
        old_ts = datetime(2000, 1, 1, tzinfo=timezone.utc)
        mock_bq.get_dag_overview.return_value = [{"dag_id": "old", "latest_execution": old_ts}]
        result = be.get_inactive_dags(inactive_days=30)
        assert len(result) == 1
        # called with hours = 30 * 24 = 720
        mock_bq.get_dag_overview.assert_called_once_with(lookback_hours=720)


# ---------------------------------------------------------------------------
# BigQueryBackend — dependency monitor interface
# ---------------------------------------------------------------------------


class TestBigQueryBackendDependencies:
    def test_get_upstream_failures_maps_rows(self):
        be, mock_bq = _make_backend()
        mock_bq.get_recent_failures.return_value = [
            {"dag_id": "d", "task_id": "t", "run_id": "r", "execution_date": None, "model_unique_id": "m"}
        ]
        result = be.get_upstream_failures(lookback_hours=12)
        assert result[0]["dag_id"] == "d"
        assert "backend_note" in result[0]

    def test_get_cross_dag_dependencies_empty(self):
        be, _ = _make_backend()
        assert be.get_cross_dag_dependencies() == []

    def test_get_failure_correlation_delegates(self):
        be, mock_bq = _make_backend()
        mock_bq.get_failure_correlations.return_value = {"correlations": [], "lookback_hours": 24}
        result = be.get_failure_correlation(lookback_hours=24)
        assert result["correlations"] == []

    def test_get_cascading_failure_impact_has_note(self):
        be, _ = _make_backend()
        result = be.get_cascading_failure_impact(dag_id="d", task_id="t")
        assert result["dag_id"] == "d"
        assert "backend_note" in result


# ---------------------------------------------------------------------------
# monitor_provider — backend sentinel logic
# ---------------------------------------------------------------------------


class TestMonitorProviderBackend:
    def test_airflow_backend_uses_airflow_monitor(self, monkeypatch):
        import airflow_watcher.api.monitor_provider as mp

        # Reset module-level singletons
        mp._backend = mp._NOT_SET
        mp._failure_monitor = None

        mock_cfg = MagicMock()
        mock_cfg.backend = "airflow"
        monkeypatch.setattr(mp, "_get_config", lambda: mock_cfg)

        mock_monitor = MagicMock()
        with patch("airflow_watcher.monitors.dag_failure_monitor.DAGFailureMonitor", return_value=mock_monitor):
            result = mp.get_failure_monitor()

        assert result is mock_monitor

        # Cleanup
        mp._backend = mp._NOT_SET
        mp._failure_monitor = None

    def test_bigquery_backend_returns_backend_for_all_getters(self, monkeypatch):
        import airflow_watcher.api.monitor_provider as mp

        mp._backend = mp._NOT_SET
        mp._failure_monitor = None
        mp._sla_monitor = None
        mp._task_monitor = None
        mp._scheduling_monitor = None
        mp._dag_health_monitor = None
        mp._dependency_monitor = None

        mock_be = MagicMock()
        monkeypatch.setattr(mp, "_get_backend", lambda: mock_be)

        assert mp.get_failure_monitor() is mock_be
        assert mp.get_sla_monitor() is mock_be
        assert mp.get_task_monitor() is mock_be
        assert mp.get_scheduling_monitor() is mock_be
        assert mp.get_dag_health_monitor() is mock_be
        assert mp.get_dependency_monitor() is mock_be

        # Cleanup
        mp._backend = mp._NOT_SET
