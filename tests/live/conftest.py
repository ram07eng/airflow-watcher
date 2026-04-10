"""Conftest for live integration tests.

Auto-skips all tests in this directory when the Airflow webserver (port 8080)
or standalone API (port 8083) is not reachable.

test_qa_plugin.py is excluded from pytest collection because its test functions
accept runtime arguments from a ``__main__`` block. Run it directly:
    python tests/live/test_qa_plugin.py
"""

import socket
from pathlib import Path

import pytest

# test_qa_plugin.py is a standalone script, not a pytest module.
collect_ignore = ["test_qa_plugin.py"]


def _port_open(port: int, host: str = "localhost", timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def pytest_collection_modifyitems(config, items):
    """Skip live tests when Docker services are not running."""
    webserver_up = _port_open(8080)
    api_up = _port_open(8083)

    if webserver_up and api_up:
        return

    reason = "Live environment not running (need Docker on ports 8080-8083)"
    skip_marker = pytest.mark.skip(reason=reason)
    live_dir = str(Path(__file__).parent)
    for item in items:
        if str(item.fspath).startswith(live_dir):
            item.add_marker(skip_marker)
