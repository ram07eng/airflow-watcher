# Airflow Watcher — BigQuery PoC

A proof-of-concept showing how **airflow-watcher** monitoring data can be served
directly from **BigQuery** instead of a Postgres metadata database.

The data source is the `morpheusdbtresult` table, populated by the dbt-airflow
callback plugin. Each row represents one dbt task invocation and contains the
full `results[]` array with per-model timings, statuses, and `adapter_response`
cost metadata.

---

## Demo

![Airflow Watcher BigQuery PoC demo](demo_output/airflow_watcher_bq_demo.gif)

---

## Endpoints

| Group | Path | What it answers |
|---|---|---|
| **Overview** | `GET /api/v1/overview` | Per-DAG run counts, failures, avg duration, BQ cost |
| **Failures** | `GET /api/v1/failures` | Raw failure rows per dbt model |
| **Failures** | `GET /api/v1/failures/stats` | Failure rates, top offending DAGs |
| **Tasks** | `GET /api/v1/tasks/long-running` | dbt invocations exceeding a time threshold |
| **Tasks** | `GET /api/v1/tasks/model-performance` | Top-N slowest models with avg/max latency |
| **Scheduling** | `GET /api/v1/scheduling/lag` | p50/p90/p95 lag between `execution_date` and first model start |
| **Dependencies** | `GET /api/v1/dependencies/task-graph/{dag_id}` | Inferred task graph from timing data |
| **Dependencies** | `GET /api/v1/dependencies/correlations` | DAG pairs that fail in the same hour |
| **dbt / Cost** | `GET /api/v1/dbt/cost` | GB billed + slot hours from `adapter_response` |

> `/api/v1/dbt/cost` has no equivalent in the Postgres version — it is only
> possible because the dbt-bigquery adapter embeds cost metadata in the callback payload.

---

## Data source

```
just-data.production_je_monitoring.morpheusdbtresult_all_2026
```

All queries use parameterised SQL and read-only credentials.

---

## Running locally

```bash
# From repo root
bash demo/bigquery/run.sh
```

API runs on `http://localhost:8090`.  
Interactive docs at `http://localhost:8090/docs`.

Override the table or port:

```bash
BQ_TABLE=my-project.dataset.table PORT=9000 bash demo/bigquery/run.sh
```

Requires Google Cloud Application Default Credentials with read access to the BQ table:

```bash
gcloud auth application-default login
```

---

## Recording a new demo

```bash
# From repo root — requires the API to be running on :8090
python3 demo/bigquery/record_demo.py
```

Output saved to `demo/bigquery/demo_output/airflow_watcher_bq_demo.mp4`.

---

## Docker

```bash
cd demo/bigquery
docker build -t airflow-watcher-bq-poc .
docker run -p 8090:8080 \
  -e BQ_TABLE=just-data.production_je_monitoring.morpheusdbtresult_all_2026 \
  -v "$HOME/.config/gcloud:/root/.config/gcloud:ro" \
  airflow-watcher-bq-poc
```
