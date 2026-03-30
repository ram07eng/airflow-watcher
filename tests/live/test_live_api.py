#!/usr/bin/env python
"""Quick smoke test for the live standalone API."""

import json
import urllib.request

API_KEY = "v8LEah9G93MOoPJn7WUUm8vYp6jEsnFmC_FOpMX1ctU"
BASE = "http://localhost:8083/api/v1"

endpoints = [
    "/failures/?hours=24&limit=3",
    "/failures/stats?hours=24",
    "/sla/misses?hours=24",
    "/sla/stats?hours=24",
    "/tasks/long-running?hours=24",
    "/tasks/retries?hours=24",
    "/tasks/zombies",
    "/tasks/failure-patterns?hours=24",
    "/scheduling/lag",
    "/scheduling/queue",
    "/scheduling/pools",
    "/scheduling/stale-dags?hours=72",
    "/scheduling/concurrent",
    "/dags/import-errors",
    "/dags/status-summary",
    "/dags/complexity",
    "/dags/inactive?days=7",
    "/dependencies/upstream-failures?hours=24",
    "/dependencies/cross-dag",
    "/dependencies/correlations?hours=24",
    "/health/",
    "/overview/",
    "/alerts/rules",
]

for ep in endpoints:
    try:
        req = urllib.request.Request(BASE + ep, headers={"Authorization": f"Bearer {API_KEY}"})
        resp = urllib.request.urlopen(req)
        data = json.loads(resp.read().decode())
        if isinstance(data.get("data"), list):
            print(f"OK  {ep:40s} -> {len(data['data'])} items")
        elif "data" in data:
            keys = list(data["data"].keys())[:5]
            print(f"OK  {ep:40s} -> keys: {keys}")
        else:
            keys = list(data.keys())[:5]
            print(f"OK  {ep:40s} -> keys: {keys}")
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:200]
        print(f"ERR {ep:40s} -> {e.code}: {body}")
    except Exception as e:
        print(f"ERR {ep:40s} -> {e}")
