"""Tests for api/compat.py — startup validation of reflected tables."""

import logging
import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

from airflow_watcher.api.compat import reflect_airflow_models


class TestReflectAirflowModelsValidation:
    """Verify startup validation logs errors for missing critical tables."""

    def _setup_stub_models(self):
        """Create minimal stub models module for testing."""
        models_mod = ModuleType("airflow.models")
        models_mod.__path__ = []
        for attr in ("DagRun", "DagModel", "TaskInstance", "SlaMiss",
                      "Pool", "ImportError", "DagTag"):
            setattr(models_mod, attr, type(attr, (), {}))

        sd_mod = ModuleType("airflow.models.serialized_dag")
        sd_mod.SerializedDagModel = type("SerializedDagModel", (), {})

        return models_mod, sd_mod

    def test_logs_error_when_critical_tables_missing(self, caplog):
        """If DagRun/TaskInstance/DagModel can't be mapped, an error is logged."""
        models_mod, sd_mod = self._setup_stub_models()
        mock_engine = MagicMock()

        # The conftest stubs sqlalchemy with MagicMock — Table() will return
        # a MagicMock which won't raise NoSuchTableError, but also won't set
        # __table__ on the class. So critical tables stay unmapped → error log.
        with patch.dict(sys.modules, {
            "airflow.models": models_mod,
            "airflow.models.serialized_dag": sd_mod,
        }):
            with caplog.at_level(logging.ERROR):
                reflect_airflow_models(mock_engine)

        assert "could not map" in caplog.text

    def test_logs_success_when_tables_mapped(self, caplog):
        """If critical tables are mapped, success is logged."""
        models_mod, sd_mod = self._setup_stub_models()
        mock_engine = MagicMock()

        # Give all stubs a __table__ attr to simulate successful mapping
        for attr in ("DagRun", "DagModel", "TaskInstance", "SlaMiss",
                      "Pool", "ImportError", "DagTag"):
            getattr(models_mod, attr).__table__ = MagicMock()
        sd_mod.SerializedDagModel.__table__ = MagicMock()

        with patch.dict(sys.modules, {
            "airflow.models": models_mod,
            "airflow.models.serialized_dag": sd_mod,
        }):
            with caplog.at_level(logging.INFO):
                reflect_airflow_models(mock_engine)

        assert "mapped successfully" in caplog.text
