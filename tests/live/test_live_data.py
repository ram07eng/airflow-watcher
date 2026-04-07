#!/usr/bin/env python
"""Show sample data from key endpoints."""

import json
import urllib.request

API_KEY = "v8LEah9G93MOoPJn7WUUm8vYp6jEsnFmC_FOpMX1ctU"
BASE = "http://localhost:8083/api/v1"


def fetch(ep):
    req = urllib.request.Request(BASE + ep, headers={"Authorization": f"Bearer {API_KEY}"})
    return json.loads(urllib.request.urlopen(req).read().decode())


if __name__ == "__main__":
    print("=" * 60)
    print("OVERVIEW")
    print("=" * 60)
    overview = fetch("/overview/")
    print(json.dumps(overview["data"], indent=2)[:1500])

    print("\n" + "=" * 60)
    print("FAILURES (top 2)")
    print("=" * 60)
    failures = fetch("/failures/?hours=24&limit=2")
    print(json.dumps(failures["data"], indent=2)[:1500])

    print("\n" + "=" * 60)
    print("DAG STATUS SUMMARY")
    print("=" * 60)
    dags = fetch("/dags/status-summary")
    print(json.dumps(dags["data"], indent=2)[:1500])

    print("\n" + "=" * 60)
    print("HEALTH SCORE")
    print("=" * 60)
    health = fetch("/health/")
    print(json.dumps(health["data"], indent=2)[:1500])
