"""Tests for api/envelope.py — response envelope helpers."""

from airflow_watcher.api.envelope import error_response, success_response


class TestSuccessResponse:
    def test_has_required_keys(self):
        result = success_response({"foo": 1})
        assert result["status"] == "success"
        assert result["data"] == {"foo": 1}
        assert "timestamp" in result

    def test_timestamp_ends_with_z(self):
        result = success_response(None)
        assert result["timestamp"].endswith("Z")

    def test_accepts_none(self):
        result = success_response(None)
        assert result["data"] is None

    def test_accepts_list(self):
        result = success_response([1, 2, 3])
        assert result["data"] == [1, 2, 3]


class TestErrorResponse:
    def test_has_required_keys(self):
        result = error_response("something went wrong")
        assert result["status"] == "error"
        assert result["error"] == "something went wrong"
        assert "timestamp" in result

    def test_timestamp_ends_with_z(self):
        result = error_response("err")
        assert result["timestamp"].endswith("Z")
