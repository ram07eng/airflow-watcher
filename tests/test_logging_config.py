"""Tests for api/logging_config.py — structured logging and request IDs."""

import json
import logging

from airflow_watcher.api.logging_config import (
    JSONFormatter,
    configure_logging,
    generate_request_id,
    get_request_id,
    set_request_id,
)


class TestGenerateRequestId:
    def test_returns_hex_string(self):
        rid = generate_request_id()
        assert isinstance(rid, str)
        int(rid, 16)  # should not raise

    def test_length_is_16(self):
        assert len(generate_request_id()) == 16

    def test_unique(self):
        ids = {generate_request_id() for _ in range(50)}
        assert len(ids) == 50


class TestRequestIdContextVar:
    def test_default_is_none(self):
        assert get_request_id() is None

    def test_set_and_get(self):
        set_request_id("abc123")
        assert get_request_id() == "abc123"
        # Reset for other tests
        set_request_id(None)


class TestJSONFormatter:
    def _make_record(self, msg="hello", level=logging.INFO):
        record = logging.LogRecord(
            name="test.logger",
            level=level,
            pathname="test.py",
            lineno=1,
            msg=msg,
            args=(),
            exc_info=None,
        )
        return record

    def test_emits_valid_json(self):
        formatter = JSONFormatter()
        record = self._make_record()
        output = formatter.format(record)
        parsed = json.loads(output)
        assert isinstance(parsed, dict)

    def test_contains_required_fields(self):
        formatter = JSONFormatter()
        record = self._make_record("test message")
        parsed = json.loads(formatter.format(record))
        assert parsed["level"] == "INFO"
        assert parsed["logger"] == "test.logger"
        assert parsed["message"] == "test message"
        assert "timestamp" in parsed

    def test_includes_request_id_when_set(self):
        set_request_id("req-42")
        try:
            formatter = JSONFormatter()
            record = self._make_record()
            parsed = json.loads(formatter.format(record))
            assert parsed["request_id"] == "req-42"
        finally:
            set_request_id(None)

    def test_omits_request_id_when_not_set(self):
        formatter = JSONFormatter()
        record = self._make_record()
        parsed = json.loads(formatter.format(record))
        assert "request_id" not in parsed

    def test_includes_exception_info(self):
        formatter = JSONFormatter()
        try:
            raise ValueError("boom")
        except ValueError:
            import sys
            record = self._make_record()
            record.exc_info = sys.exc_info()
        parsed = json.loads(formatter.format(record))
        assert "exception" in parsed
        assert "ValueError" in parsed["exception"]


class TestConfigureLogging:
    def test_json_format_uses_json_formatter(self):
        configure_logging(log_format="json", log_level="DEBUG")
        root = logging.getLogger()
        assert any(isinstance(h.formatter, JSONFormatter) for h in root.handlers)

    def test_text_format_does_not_use_json_formatter(self):
        configure_logging(log_format="text", log_level="INFO")
        root = logging.getLogger()
        assert not any(isinstance(h.formatter, JSONFormatter) for h in root.handlers)

    def test_sets_log_level(self):
        configure_logging(log_format="text", log_level="WARNING")
        root = logging.getLogger()
        assert root.level == logging.WARNING
        # Reset
        configure_logging(log_format="text", log_level="INFO")
