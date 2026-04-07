# Airflow Watcher — Load Test Results

**Date:** 2026-03-13
**Environment:** Docker single-container (Airflow 2.7.3, Python 3.10, 1 gunicorn worker)
**Plugin version:** 0.1.2 with RBAC integration

## Test Configuration

| Parameter | Value |
|-----------|-------|
| Virtual users | 100 |
| Concurrent threads | 20 |
| User roles | admin (34), weather (33), ecommerce (33) |
| UI endpoints | 8 |
| API endpoints | 16 |
| Total requests | 2,400 |

### Endpoints Tested

**UI Pages:**
- `/watcher/dashboard`, `/watcher/failures`, `/watcher/sla`, `/watcher/health`
- `/watcher/tasks`, `/watcher/scheduling`, `/watcher/dag-health`, `/watcher/dependencies`

**API Routes:**
- `/api/watcher/failures`, `/api/watcher/failures/stats`
- `/api/watcher/sla/misses`, `/api/watcher/sla/stats`
- `/api/watcher/health`, `/api/watcher/overview`
- `/api/watcher/tasks/long-running`, `/api/watcher/tasks/retries`, `/api/watcher/tasks/zombies`
- `/api/watcher/dags/status-summary`, `/api/watcher/dags/complexity`
- `/api/watcher/scheduling/lag`, `/api/watcher/scheduling/queue`, `/api/watcher/scheduling/pools`
- `/api/watcher/dependencies/upstream-failures`, `/api/watcher/dependencies/cross-dag`

## Overall Results

| Metric | Value |
|--------|-------|
| Success rate | **100%** (2,400/2,400) |
| Throughput | **205.2 req/s** |
| Total wall time | 11.7s |
| Real errors | 0 |

### Response Times

| Percentile | Time |
|------------|------|
| Min | 9 ms |
| Average | 97 ms |
| Median (P50) | 83 ms |
| P95 | 173 ms |
| P99 | 218 ms |
| Max | 329 ms |

## Per-Endpoint Breakdown

### UI Pages

| Endpoint | Avg (ms) | P95 (ms) | Max (ms) | Errors |
|----------|----------|----------|----------|--------|
| /watcher/dashboard | 165 | 279 | 328 | 0 |
| /watcher/failures | 163 | 233 | 266 | 0 |
| /watcher/sla | 161 | 199 | 204 | 0 |
| /watcher/health | 145 | 179 | 191 | 0 |
| /watcher/tasks | 153 | 194 | 205 | 0 |
| /watcher/scheduling | 137 | 165 | 182 | 0 |
| /watcher/dag-health | 111 | 135 | 146 | 0 |
| /watcher/dependencies | 106 | 125 | 131 | 0 |

### API Endpoints

| Endpoint | Avg (ms) | P95 (ms) | Max (ms) | Errors |
|----------|----------|----------|----------|--------|
| /api/watcher/failures | 94 | 112 | 119 | 0 |
| /api/watcher/failures/stats | 81 | 98 | 106 | 0 |
| /api/watcher/sla/misses | 80 | 95 | 108 | 0 |
| /api/watcher/sla/stats | 89 | 106 | 108 | 0 |
| /api/watcher/health | 74 | 93 | 100 | 0 |
| /api/watcher/overview | 76 | 120 | 136 | 0 |
| /api/watcher/tasks/long-running | 71 | 102 | 121 | 0 |
| /api/watcher/tasks/retries | 75 | 104 | 269 | 0 |
| /api/watcher/tasks/zombies | 71 | 102 | 110 | 0 |
| /api/watcher/dags/status-summary | 63 | 94 | 116 | 0 |
| /api/watcher/dags/complexity | 70 | 117 | 272 | 0 |
| /api/watcher/scheduling/lag | 71 | 100 | 116 | 0 |
| /api/watcher/scheduling/queue | 62 | 82 | 109 | 0 |
| /api/watcher/scheduling/pools | 61 | 84 | 134 | 0 |
| /api/watcher/dependencies/upstream-failures | 72 | 140 | 329 | 0 |
| /api/watcher/dependencies/cross-dag | 75 | 142 | 234 | 0 |

## Per-Role Performance (RBAC Overhead)

| Role | Users | Avg (ms) | P95 (ms) | Errors |
|------|-------|----------|----------|--------|
| admin | 34 | 95 | 170 | 0 |
| weather | 33 | 98 | 173 | 0 |
| ecommerce | 33 | 98 | 176 | 0 |

RBAC filtering adds negligible overhead (~3ms difference between admin and restricted users).

## Notes

- `/api/watcher/health` returns HTTP 503 when health score < 70 — this is by design, not an error.
- Sessions are shared per role (3 authenticated sessions for 100 virtual users), simulating production behavior where users maintain persistent sessions.
- The test environment is a single Docker container with 1 gunicorn worker — production deployments with multiple workers would show better concurrency.

## Running the Test

```bash
cd demo/load-tests
pip install requests  # only dependency
python load_test.py 100       # 100 users (default)
python load_test.py 50        # or any number
```
