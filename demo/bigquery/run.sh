#!/usr/bin/env bash
# Launch the BigQuery proof-of-concept API.
#
# Usage:
#   ./run.sh                                        # use defaults
#   AIRFLOW_WATCHER_BQ_TABLE=my-project.ds.t PORT=9000 ./run.sh

set -euo pipefail
cd "$(dirname "$0")/../.."   # repo root

export AIRFLOW_WATCHER_BQ_TABLE="${AIRFLOW_WATCHER_BQ_TABLE:-just-data.production_je_monitoring.morpheusdbtresult_all_2026}"
export PORT="${PORT:-8090}"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Airflow Watcher — BigQuery PoC                            ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Table : ${AIRFLOW_WATCHER_BQ_TABLE}"
echo "║  Port  : ${PORT}"
echo "║  Docs  : http://localhost:${PORT}/docs"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

python -m uvicorn demo.bigquery.app:app --host 0.0.0.0 --port "${PORT}" --reload
