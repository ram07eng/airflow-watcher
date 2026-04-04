"""Tests for utility helpers."""

import pytest
from datetime import datetime, timedelta

from airflow_watcher.utils.helpers import (
    format_duration,
    format_datetime,
    time_ago,
    truncate_string,
    get_severity_color,
    calculate_success_rate,
)


class TestHelpers:
    """Test cases for utility helpers."""

    def test_format_duration_seconds(self):
        """Test formatting seconds."""
        assert format_duration(30) == "30.0s"
        assert format_duration(45.5) == "45.5s"

    def test_format_duration_minutes(self):
        """Test formatting minutes."""
        assert format_duration(120) == "2.0m"
        assert format_duration(90) == "1.5m"

    def test_format_duration_hours(self):
        """Test formatting hours."""
        assert format_duration(3600) == "1.0h"
        assert format_duration(7200) == "2.0h"

    def test_format_duration_none(self):
        """Test formatting None value."""
        assert format_duration(None) == "N/A"

    def test_format_datetime(self):
        """Test datetime formatting."""
        dt = datetime(2024, 1, 15, 10, 30, 45)
        assert format_datetime(dt) == "2024-01-15 10:30:45"

    def test_format_datetime_custom_format(self):
        """Test datetime with custom format."""
        dt = datetime(2024, 1, 15, 10, 30, 45)
        assert format_datetime(dt, "%Y-%m-%d") == "2024-01-15"

    def test_format_datetime_none(self):
        """Test formatting None datetime."""
        assert format_datetime(None) == "N/A"

    def test_time_ago_just_now(self):
        """Test time ago for recent time."""
        now = datetime.utcnow()
        assert time_ago(now) == "just now"

    def test_time_ago_minutes(self):
        """Test time ago for minutes."""
        dt = datetime.utcnow() - timedelta(minutes=5)
        assert time_ago(dt) == "5m ago"

    def test_time_ago_hours(self):
        """Test time ago for hours."""
        dt = datetime.utcnow() - timedelta(hours=3)
        assert time_ago(dt) == "3h ago"

    def test_time_ago_days(self):
        """Test time ago for days."""
        dt = datetime.utcnow() - timedelta(days=2)
        assert time_ago(dt) == "2d ago"

    def test_time_ago_none(self):
        """Test time ago for None."""
        assert time_ago(None) == "N/A"

    def test_truncate_string_short(self):
        """Test truncating short string."""
        result = truncate_string("hello", 10)
        assert result == "hello"

    def test_truncate_string_long(self):
        """Test truncating long string."""
        result = truncate_string("hello world this is a long string", 15)
        assert result == "hello world ..."
        assert len(result) == 15

    def test_get_severity_color_green(self):
        """Test green severity color."""
        assert get_severity_color(0) == "#4caf50"

    def test_get_severity_color_orange(self):
        """Test orange severity color."""
        assert get_severity_color(1) == "#ff9800"
        assert get_severity_color(2) == "#ff9800"

    def test_get_severity_color_red(self):
        """Test red severity color."""
        assert get_severity_color(3) == "#f44336"
        assert get_severity_color(10) == "#f44336"

    def test_calculate_success_rate_full_success(self):
        """Test 100% success rate."""
        assert calculate_success_rate(10, 0) == 100.0

    def test_calculate_success_rate_partial(self):
        """Test partial success rate."""
        assert calculate_success_rate(10, 3) == 70.0

    def test_calculate_success_rate_all_failed(self):
        """Test 0% success rate."""
        assert calculate_success_rate(10, 10) == 0.0

    def test_calculate_success_rate_empty(self):
        """Test success rate with no runs."""
        assert calculate_success_rate(0, 0) == 100.0
