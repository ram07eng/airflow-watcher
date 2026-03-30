"""Utilities package for Airflow Watcher."""


def __getattr__(name):
    """Lazy-import helpers to avoid pulling in Airflow when only cache is needed."""
    if name in ("format_duration", "format_datetime"):
        from airflow_watcher.utils.helpers import format_duration, format_datetime

        _exports = {"format_duration": format_duration, "format_datetime": format_datetime}
        return _exports[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["format_duration", "format_datetime"]
