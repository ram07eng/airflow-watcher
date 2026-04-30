"""Proof-of-concept FastAPI app that serves airflow-watcher–style
endpoints backed by the BigQuery morpheusdbtresult table.

Run:
    uvicorn demo.bigquery.app:app --reload --port 8090

Or:
    python -m demo.bigquery.app
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import HTMLResponse, JSONResponse

import sys
from pathlib import Path

# Allow running standalone — ensure bq_client is importable
_here = Path(__file__).resolve().parent
if str(_here) not in sys.path:
    sys.path.insert(0, str(_here))

from bq_client import BQClient  # noqa: E402

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------
_bq: Optional[BQClient] = None
_start_time: float = 0.0
_BQ_TABLE = os.getenv(
    "BQ_TABLE",
    "just-data.production_je_monitoring.morpheusdbtresult_all_2026",
)

# ---------------------------------------------------------------------------
# Tag metadata — shown as sections in Swagger UI
# ---------------------------------------------------------------------------
_TAGS = [
    {
        "name": "overview",
        "description": (
            "High-level per-DAG summary across a rolling time window. "
            "Good first stop — shows run counts, failure counts, avg duration, and estimated BigQuery cost per DAG."
        ),
    },
    {
        "name": "failures",
        "description": (
            "Drill into dbt model failures. "
            "`/failures` returns the raw failure rows; `/failures/stats` aggregates them into rates, "
            "top offenders, and total model counts."
        ),
    },
    {
        "name": "tasks",
        "description": (
            "Task-level health signals: long-running dbt invocations and per-model performance profiles "
            "(avg/max/min execution time, GB billed, slot usage)."
        ),
    },
    {
        "name": "scheduling",
        "description": (
            "Scheduling lag analysis — measures the delay between a DAG's `execution_date` and the moment "
            "the first dbt model actually starts executing. Surfacing p50/p90/p95 latency."
        ),
    },
    {
        "name": "dependencies",
        "description": (
            "Two dependency views: (1) an **inferred task graph** reconstructed from timing data for a single "
            "DAG run, and (2) **failure correlations** — DAG pairs that fail within the same hour, "
            "suggesting shared upstream dependencies."
        ),
    },
    {
        "name": "dbt",
        "description": (
            "dbt-specific endpoints that have no equivalent in the Postgres version of airflow-watcher. "
            "Cost analysis reads `adapter_response.bytes_billed` and `slot_ms` written by the dbt-bigquery adapter."
        ),
    },
    {
        "name": "system",
        "description": "Liveness probe and runtime metadata.",
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _ts() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def success(data: Any) -> dict:
    return {"status": "success", "data": data, "timestamp": _ts()}


def error(message: str, status_code: int = 500) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"status": "error", "error": message, "timestamp": _ts()},
    )


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _bq, _start_time
    _start_time = time.monotonic()
    _bq = BQClient(table=_BQ_TABLE)
    logger.info("BQ PoC started — table: %s", _BQ_TABLE)
    yield
    logger.info("BQ PoC shutting down")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
_DESCRIPTION = """\
## What is this?

A proof-of-concept showing how **airflow-watcher** monitoring data can be served
directly from **BigQuery** instead of a Postgres database.

The data source is the `morpheusdbtresult` table, populated by the
[dbt-airflow callback plugin](https://github.com/anomalyco/airflow-watcher).
Each row represents one dbt task invocation and contains the full
`results[]` array with per-model timings, statuses, and `adapter_response` cost metadata.

---

## Key capabilities

| Endpoint group | What it answers |
|---|---|
| **Overview** | Which DAGs ran, how many models, any failures, estimated cost |
| **Failures** | Which dbt models errored, failure rates, top offending DAGs |
| **Tasks** | Long-running invocations, per-model avg/max latency |
| **Scheduling** | How late does Airflow actually start executing after the scheduled time? |
| **Dependencies** | Inferred task graph from timing; correlated failure pairs |
| **dbt / Cost** | GB billed + slot hours — BigQuery-native, no Postgres equivalent |

---

## Data source

```
just-data.production_je_monitoring.morpheusdbtresult_all_2026
```

All queries use parameterised SQL and read-only credentials.
"""

app = FastAPI(
    title="Airflow Watcher — BigQuery PoC",
    description=_DESCRIPTION,
    version="0.1.0-poc",
    openapi_tags=_TAGS,
    lifespan=lifespan,
    contact={
        "name": "Data Engineering — JET",
        "email": "ramanujam.solaimalai@justeattakeaway.com",
    },
    license_info={
        "name": "Internal PoC — not for production use",
    },
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Landing page
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def landing():
    table_short = _BQ_TABLE.split(".")[-1]
    uptime = round(time.monotonic() - _start_time, 1)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Airflow Watcher — BigQuery PoC</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #0f1117;
      color: #e2e8f0;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      align-items: center;
      padding: 48px 24px 80px;
    }}
    .badge {{
      display: inline-block;
      background: #ff6b35;
      color: white;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: .08em;
      text-transform: uppercase;
      padding: 3px 10px;
      border-radius: 999px;
      margin-bottom: 20px;
    }}
    h1 {{
      font-size: 2.2rem;
      font-weight: 700;
      letter-spacing: -.02em;
      text-align: center;
      line-height: 1.2;
      margin-bottom: 12px;
    }}
    h1 span {{ color: #ff6b35; }}
    .subtitle {{
      color: #94a3b8;
      font-size: 1rem;
      text-align: center;
      max-width: 560px;
      line-height: 1.6;
      margin-bottom: 36px;
    }}
    .meta {{
      display: flex;
      gap: 16px;
      flex-wrap: wrap;
      justify-content: center;
      margin-bottom: 48px;
    }}
    .chip {{
      background: #1e2433;
      border: 1px solid #2d3748;
      border-radius: 8px;
      padding: 8px 16px;
      font-size: 13px;
      color: #94a3b8;
    }}
    .chip strong {{ color: #e2e8f0; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
      gap: 16px;
      width: 100%;
      max-width: 900px;
      margin-bottom: 48px;
    }}
    .card {{
      background: #1e2433;
      border: 1px solid #2d3748;
      border-radius: 12px;
      padding: 20px 24px;
      transition: border-color .15s;
    }}
    .card:hover {{ border-color: #ff6b35; }}
    .card-tag {{
      display: inline-block;
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: .08em;
      color: #ff6b35;
      margin-bottom: 8px;
    }}
    .card h3 {{
      font-size: 15px;
      font-weight: 600;
      margin-bottom: 6px;
      color: #f1f5f9;
    }}
    .card p {{
      font-size: 13px;
      color: #64748b;
      line-height: 1.5;
    }}
    .card code {{
      font-family: "SF Mono", "Fira Code", monospace;
      font-size: 12px;
      background: #0f1117;
      border: 1px solid #2d3748;
      border-radius: 4px;
      padding: 1px 6px;
      color: #94a3b8;
      display: block;
      margin-top: 10px;
    }}
    .cta {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      background: #ff6b35;
      color: white;
      font-weight: 600;
      font-size: 15px;
      padding: 12px 28px;
      border-radius: 10px;
      text-decoration: none;
      transition: background .15s, transform .1s;
    }}
    .cta:hover {{ background: #e85d28; transform: translateY(-1px); }}
    .cta-secondary {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      background: transparent;
      color: #94a3b8;
      font-weight: 500;
      font-size: 15px;
      padding: 12px 28px;
      border-radius: 10px;
      border: 1px solid #2d3748;
      text-decoration: none;
      margin-left: 12px;
      transition: border-color .15s, color .15s;
    }}
    .cta-secondary:hover {{ border-color: #94a3b8; color: #e2e8f0; }}
    .actions {{ display: flex; flex-wrap: wrap; gap: 12px; justify-content: center; }}
  </style>
</head>
<body>
  <div class="badge">Proof of Concept</div>
  <h1>Airflow Watcher<br><span>BigQuery Backend</span></h1>
  <p class="subtitle">
    Monitoring API for dbt + Airflow pipelines — powered by BigQuery instead of Postgres.
    Live data from <strong>{table_short}</strong>.
  </p>

  <div class="meta">
    <div class="chip">Status: <strong>Healthy</strong></div>
    <div class="chip">Uptime: <strong>{uptime}s</strong></div>
    <div class="chip">Source: <strong>BigQuery</strong></div>
    <div class="chip">Version: <strong>0.1.0-poc</strong></div>
  </div>

  <div class="grid">
    <div class="card">
      <span class="card-tag">Overview</span>
      <h3>DAG Summary</h3>
      <p>Per-DAG run counts, failure totals, avg duration, and estimated BQ cost over a rolling window.</p>
      <code>GET /api/v1/overview</code>
    </div>
    <div class="card">
      <span class="card-tag">Failures</span>
      <h3>Model Failures</h3>
      <p>Raw failure rows per dbt model, plus aggregate stats: rates, top offending DAGs, total counts.</p>
      <code>GET /api/v1/failures/stats</code>
    </div>
    <div class="card">
      <span class="card-tag">Tasks</span>
      <h3>Long-Running Tasks</h3>
      <p>dbt invocations exceeding a time threshold, plus per-model avg/max latency profiles.</p>
      <code>GET /api/v1/tasks/long-running</code>
    </div>
    <div class="card">
      <span class="card-tag">Scheduling</span>
      <h3>Scheduling Lag</h3>
      <p>p50/p90/p95 delay between scheduled execution_date and actual first model start time.</p>
      <code>GET /api/v1/scheduling/lag</code>
    </div>
    <div class="card">
      <span class="card-tag">Dependencies</span>
      <h3>Task Graph &amp; Correlations</h3>
      <p>Inferred task dependency graph from timing, plus DAG pairs that fail in the same hour.</p>
      <code>GET /api/v1/dependencies/correlations</code>
    </div>
    <div class="card">
      <span class="card-tag">dbt · BigQuery-native</span>
      <h3>Cost Analysis</h3>
      <p>GB billed and slot hours from <code>adapter_response</code> metadata. No equivalent in the Postgres version.</p>
      <code>GET /api/v1/dbt/cost</code>
    </div>
  </div>

  <div class="actions">
    <a class="cta" href="/docs">Open Interactive API Docs &rarr;</a>
    <a class="cta-secondary" href="/healthz">Health Check</a>
  </div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------
@app.get(
    "/healthz",
    tags=["system"],
    summary="Liveness probe",
    response_description="Service status and uptime",
)
async def healthz():
    """Returns `healthy` plus uptime and the BigQuery table being queried.

    Use this to verify the service is up before running the demo.
    """
    uptime = round(time.monotonic() - _start_time, 1)
    return {
        "status": "healthy",
        "uptime_seconds": uptime,
        "data_source": "bigquery",
        "table": _BQ_TABLE,
    }


# ---------------------------------------------------------------------------
# 1. Failures
# ---------------------------------------------------------------------------
@app.get(
    "/api/v1/failures",
    tags=["failures"],
    summary="List recent dbt model failures",
    response_description="Paginated list of failed dbt model runs",
)
async def get_failures(
    dag_id: Optional[str] = Query(
        None,
        max_length=250,
        description="Filter by a specific DAG ID. Leave blank to return failures across all DAGs.",
        example="dbt_je_monitoring",
    ),
    hours: int = Query(
        24,
        ge=1,
        le=8760,
        description="Lookback window in hours. Default 24h. Max 8760 (1 year).",
        example=24,
    ),
    limit: int = Query(50, ge=1, le=500, description="Page size.", example=50),
    offset: int = Query(0, ge=0, description="Page offset for pagination.", example=0),
):
    """Return dbt model runs where `status = 'error'` or `failures > 0`.

    Each row represents a single dbt model that failed within a task invocation.
    Includes the `dag_id`, `task_id`, `run_id`, `execution_date`, and per-model
    error message and failure count.

    **Tip:** start with `/api/v1/failures/stats` for an aggregate view, then drill
    into specific DAGs using the `dag_id` filter here.
    """
    assert _bq is not None
    rows = _bq.get_recent_failures(dag_id=dag_id, lookback_hours=hours, limit=limit, offset=offset)
    return success(
        {
            "failures": _serialise_rows(rows),
            "count": len(rows),
            "filters": {"dag_id": dag_id, "hours": hours},
            "pagination": {"offset": offset, "limit": limit},
            "source": "bigquery",
        }
    )


@app.get(
    "/api/v1/failures/stats",
    tags=["failures"],
    summary="Aggregate failure statistics",
    response_description="Failure rates, counts, and top offending DAGs",
)
async def get_failure_stats(
    hours: int = Query(
        24,
        ge=1,
        le=8760,
        description="Lookback window in hours.",
        example=24,
    ),
):
    """Aggregate failure statistics over the lookback window.

    Returns:
    - `total_task_runs` — total dbt invocations in window
    - `task_runs_with_failures` — how many had at least one failed model
    - `failure_rate` — fraction of runs with failures (0–1)
    - `total_dbt_models_executed` / `total_dbt_models_failed`
    - `most_failing_dags` — top 10 DAGs ranked by failure count

    **Good for a demo overview slide.**
    """
    assert _bq is not None
    stats = _bq.get_failure_statistics(lookback_hours=hours)
    return success(_serialise_row(stats))


# ---------------------------------------------------------------------------
# 2. Task health
# ---------------------------------------------------------------------------
@app.get(
    "/api/v1/tasks/long-running",
    tags=["tasks"],
    summary="Long-running dbt task invocations",
    response_description="Task runs exceeding the elapsed-time threshold",
)
async def get_long_running(
    threshold_minutes: int = Query(
        60,
        ge=1,
        le=10080,
        description="Minimum elapsed time in minutes to be considered long-running.",
        example=60,
    ),
):
    """Find dbt task invocations whose total `elapsed_time` exceeded the threshold.

    Returns `elapsed_minutes`, number of models executed, and the IDs of the
    three slowest models within each invocation.

    Covers the past 48 hours regardless of the threshold setting.
    """
    assert _bq is not None
    rows = _bq.get_long_running_tasks(threshold_minutes=threshold_minutes)
    return success(
        {
            "tasks": _serialise_rows(rows),
            "count": len(rows),
            "threshold_minutes": threshold_minutes,
            "source": "bigquery",
        }
    )


@app.get(
    "/api/v1/tasks/model-performance",
    tags=["tasks", "dbt"],
    summary="Per-model execution time profile",
    response_description="Top-N slowest dbt models with avg/max/min latency and cost",
)
async def get_model_performance(
    hours: int = Query(
        24,
        ge=1,
        le=8760,
        description="Lookback window in hours.",
        example=24,
    ),
    top_n: int = Query(
        20,
        ge=1,
        le=200,
        description="Number of models to return, ordered by avg execution time descending.",
        example=20,
    ),
):
    """Top-N slowest dbt models ranked by average execution time.

    Each row includes:
    - `avg_seconds`, `max_seconds`, `min_seconds` — execution time stats
    - `avg_gb_billed`, `avg_slot_seconds` — BigQuery cost signals
    - `failure_count` — how many times this model errored in the window

    **Combines performance and cost in one view — unique to BigQuery backend.**
    """
    assert _bq is not None
    rows = _bq.get_model_performance(lookback_hours=hours, top_n=top_n)
    return success(
        {
            "models": _serialise_rows(rows),
            "count": len(rows),
            "source": "bigquery",
        }
    )


# ---------------------------------------------------------------------------
# 3. Scheduling lag
# ---------------------------------------------------------------------------
@app.get(
    "/api/v1/scheduling/lag",
    tags=["scheduling"],
    summary="Scheduling lag percentiles",
    response_description="p50/p90/p95 delay between scheduled time and first model execution",
)
async def get_scheduling_lag(
    hours: int = Query(
        24,
        ge=1,
        le=8760,
        description="Lookback window in hours.",
        example=24,
    ),
    threshold_minutes: int = Query(
        10,
        ge=1,
        le=10080,
        description="Lag threshold in minutes. DAG runs exceeding this are listed in `delayed_dags`.",
        example=10,
    ),
):
    """Measures how long after the scheduled `execution_date` the first dbt model
    actually begins executing (i.e. Airflow queue + worker startup overhead).

    Returns:
    - `p50_lag_minutes`, `p90_lag_minutes`, `p95_lag_minutes`
    - `avg_lag_minutes`, `max_lag_minutes`
    - `delayed_dags` — individual runs exceeding `threshold_minutes`
    """
    assert _bq is not None
    data = _bq.get_scheduling_lag(lookback_hours=hours, lag_threshold_minutes=threshold_minutes)
    return success(_serialise_row(data))


# ---------------------------------------------------------------------------
# 4. Dependencies
# ---------------------------------------------------------------------------
@app.get(
    "/api/v1/dependencies/task-graph/{dag_id}",
    tags=["dependencies"],
    summary="Inferred task dependency graph",
    response_description="Tasks as nodes, temporal dependencies as edges",
)
async def get_task_graph(
    dag_id: str,
    run_id: Optional[str] = Query(
        None,
        description="Specific run ID. Omit to use the most recent run for this DAG.",
        example=None,
    ),
):
    """Reconstructs an approximate task dependency graph for a single DAG run
    by analysing the temporal ordering of task start/end times.

    An edge `A → B` is inferred when task A finished before task B started,
    with no intermediate task C that fully explains the dependency (transitive
    reduction applied).

    **Note:** this is a heuristic — it works well for linear and fan-out patterns
    but may miss parallelism in complex DAGs.
    """
    assert _bq is not None
    if run_id is None:
        from google.cloud import bigquery as _bq_mod

        sql = f"""
        SELECT run_id
        FROM `{_BQ_TABLE}`
        WHERE dag_id = @dag_id
        ORDER BY execution_date DESC
        LIMIT 1
        """
        params = [_bq_mod.ScalarQueryParameter("dag_id", "STRING", dag_id)]
        rows = _bq._run(sql, params)
        if not rows:
            return error(f"No runs found for dag_id={dag_id}", 404)
        run_id = rows[0]["run_id"]

    graph = _bq.get_inferred_task_graph(dag_id=dag_id, run_id=run_id)
    return success(graph)


@app.get(
    "/api/v1/dependencies/correlations",
    tags=["dependencies"],
    summary="Correlated DAG failure pairs",
    response_description="DAG pairs that fail within the same hour, suggesting shared dependencies",
)
async def get_failure_correlations(
    hours: int = Query(
        48,
        ge=1,
        le=8760,
        description="Lookback window in hours. Defaults to 48h for broader correlation signal.",
        example=48,
    ),
):
    """Finds pairs of DAGs that fail within the same clock hour more than once.

    High `co_occurrences` suggests the two DAGs share an upstream dependency
    (e.g. same source table, same BigQuery slot pool) that fails together.

    Only pairs with `co_occurrences >= 2` are returned to reduce noise.
    """
    assert _bq is not None
    data = _bq.get_failure_correlations(lookback_hours=hours)
    return success(_serialise_row(data))


# ---------------------------------------------------------------------------
# 5. Overview
# ---------------------------------------------------------------------------
@app.get(
    "/api/v1/overview",
    tags=["overview"],
    summary="Per-DAG activity summary",
    response_description="One row per DAG: runs, tasks, failures, cost, latest execution",
)
async def get_overview(
    hours: int = Query(
        24,
        ge=1,
        le=8760,
        description="Lookback window in hours.",
        example=24,
    ),
):
    """Returns one summary row per DAG seen in the lookback window.

    Each row includes:
    - `total_runs` — distinct run IDs
    - `unique_tasks` — distinct task IDs within those runs
    - `total_dbt_models` / `failed_models`
    - `avg_elapsed_seconds` — mean task duration
    - `total_gb_billed` — aggregate BigQuery cost signal
    - `latest_execution` — timestamp of most recent run

    **Start here for a dashboard-style overview.**
    """
    assert _bq is not None
    rows = _bq.get_dag_overview(lookback_hours=hours)
    return success(
        {
            "dags": _serialise_rows(rows),
            "dag_count": len(rows),
            "lookback_hours": hours,
            "source": "bigquery",
        }
    )


# ---------------------------------------------------------------------------
# 6. dbt cost analysis
# ---------------------------------------------------------------------------
@app.get(
    "/api/v1/dbt/cost",
    tags=["dbt"],
    summary="BigQuery cost analysis",
    response_description="Aggregate and per-model GB billed, slot hours, and execution time",
)
async def get_cost_analysis(
    hours: int = Query(
        24,
        ge=1,
        le=8760,
        description="Lookback window in hours.",
        example=24,
    ),
):
    """Reads `adapter_response.bytes_billed` and `slot_ms` written by the
    dbt-bigquery adapter into the results array.

    Summary fields:
    - `total_gb_billed` — aggregate data scanned across all models
    - `total_slot_hours` — aggregate slot consumption
    - `avg_gb_per_model` — average per dbt model execution

    Also returns `top_cost_models` — the 15 most expensive models by total GB billed.

    **This endpoint has no equivalent in the Postgres version of airflow-watcher.**
    It is only possible because the dbt-bigquery adapter embeds cost metadata
    in the callback payload.
    """
    assert _bq is not None
    data = _bq.get_cost_analysis(lookback_hours=hours)
    return success(_serialise_row(data))


# ---------------------------------------------------------------------------
# Serialisation helpers — BQ Row objects → JSON-safe dicts
# ---------------------------------------------------------------------------
def _serialise_value(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, list):
        return [_serialise_value(i) for i in v]
    if isinstance(v, dict):
        return {k: _serialise_value(val) for k, val in v.items()}
    return v


def _serialise_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _serialise_value(v) for k, v in row.items()}


def _serialise_rows(rows: list) -> list:
    return [_serialise_row(r) if isinstance(r, dict) else r for r in rows]


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main() -> None:
    import uvicorn

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    port = int(os.getenv("PORT", "8090"))
    logger.info("Starting BQ PoC on port %d", port)
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
