"""Prometheus metrics export endpoint."""

from typing import Optional

from fastapi import APIRouter, Depends
from fastapi.responses import PlainTextResponse

from airflow_watcher.api.auth import require_auth
from airflow_watcher.metrics.prometheus_exporter import PrometheusExporter

router = APIRouter(tags=["metrics"])

# Module-level reference configured at startup.
_exporter: Optional[PrometheusExporter] = None


def configure_metrics(exporter: PrometheusExporter) -> None:
    """Set the Prometheus exporter.  Called once at startup."""
    global _exporter
    _exporter = exporter


def _get_exporter() -> PrometheusExporter:
    global _exporter
    if _exporter is None:
        _exporter = PrometheusExporter()
    return _exporter


@router.get("/metrics")
def prometheus_metrics(
    _auth: Optional[str] = Depends(require_auth),
):
    """Prometheus exposition format metrics."""
    exporter = _get_exporter()
    text = exporter.generate_metrics()
    return PlainTextResponse(content=text, media_type="text/plain; version=0.0.4; charset=utf-8")
