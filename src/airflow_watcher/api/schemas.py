"""Pydantic response models for key API endpoints.

These models document the expected shape of API responses and enable
FastAPI's automatic OpenAPI schema generation.  Endpoints that return
dynamic monitor data still use ``dict`` internally; these schemas
describe the *envelope* structure so clients can rely on typed contracts.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class Envelope(BaseModel):
    """Standard JSON envelope wrapping every API response."""

    status: str = Field("success", description="'success' or 'error'")
    data: Any = Field(None, description="Response payload (varies by endpoint)")
    error: Optional[str] = Field(None, description="Error message when status='error'")
    timestamp: Optional[str] = Field(None, description="ISO 8601 UTC timestamp")


class HealthPayload(BaseModel):
    """Payload inside the envelope for GET /health."""

    status: str = Field(..., description="'healthy' or 'degraded'")
    health_score: float = Field(..., ge=0, le=100)
    summary: Dict[str, Any] = Field(default_factory=dict)
    dag_health: Dict[str, Any] = Field(default_factory=dict)
    import_error_count: int = Field(..., ge=0)


class FailureStatsPayload(BaseModel):
    """Payload for GET /failures/stats."""

    total_failures: int = Field(0, ge=0)
    unique_dags_affected: int = Field(0, ge=0)
    most_failing_dags: List[Dict[str, Any]] = Field(default_factory=list)
    failure_trend: Optional[str] = None


class OverviewPayload(BaseModel):
    """Payload for GET /overview."""

    failure_stats: Dict[str, Any] = Field(default_factory=dict)
    sla_stats: Dict[str, Any] = Field(default_factory=dict)
    long_running_tasks: int = Field(0, ge=0)
    zombie_count: int = Field(0, ge=0)
    queue_status: Dict[str, Any] = Field(default_factory=dict)
    dag_summary: Dict[str, Any] = Field(default_factory=dict)
    import_errors: int = Field(0, ge=0)
