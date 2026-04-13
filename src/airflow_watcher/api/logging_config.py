"""Structured JSON logging for the standalone API."""

import json
import logging
import sys
import uuid
from contextvars import ContextVar
from typing import Optional

# Per-request context variable for request IDs.
_request_id: ContextVar[Optional[str]] = ContextVar("request_id", default=None)


def get_request_id() -> Optional[str]:
    """Return the current request ID (or None outside a request)."""
    return _request_id.get()


def set_request_id(rid: str) -> None:
    """Set the current request's ID."""
    _request_id.set(rid)


def generate_request_id() -> str:
    """Generate a new unique request ID."""
    return uuid.uuid4().hex[:16]


class JSONFormatter(logging.Formatter):
    """Emit each log record as a single-line JSON object.

    Fields: timestamp, level, logger, message, request_id (when available).
    """

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        rid = _request_id.get()
        if rid:
            entry["request_id"] = rid
        if record.exc_info and record.exc_info[1]:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def configure_logging(log_format: str = "text", log_level: str = "INFO") -> None:
    """Configure root logger.

    Args:
        log_format: ``"json"`` for structured JSON, ``"text"`` for human-readable.
        log_level: Standard Python log level name.
    """
    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Remove existing handlers to avoid duplicates on reload
    for h in root.handlers[:]:
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stderr)
    if log_format.lower() == "json":
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s [%(name)s] %(message)s"))
    root.addHandler(handler)

    # Quieten noisy libraries
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
