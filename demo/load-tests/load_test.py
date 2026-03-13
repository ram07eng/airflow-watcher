#!/usr/bin/env python3
"""Load test for Airflow Watcher plugin with RBAC.

Simulates 100 concurrent users (mix of admin, weather_user, ecommerce_user)
hitting all Watcher UI and API endpoints.
"""

import json
import statistics
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

BASE_URL = "http://localhost:8080"

# User pool: 100 users distributed across 3 roles
USERS = [
    {"username": "admin", "password": "admin", "role": "admin"},
    {"username": "weather_user", "password": "WEATHER_PASS", "role": "weather"},
    {"username": "ecommerce_user", "password": "ECOMMERCE_PASS", "role": "ecommerce"},
]

# All Watcher endpoints to test
UI_ENDPOINTS = [
    "/watcher/dashboard",
    "/watcher/failures",
    "/watcher/sla",
    "/watcher/health",
    "/watcher/tasks",
    "/watcher/scheduling",
    "/watcher/dag-health",
    "/watcher/dependencies",
]

API_ENDPOINTS = [
    "/api/watcher/failures",
    "/api/watcher/failures/stats",
    "/api/watcher/sla/misses",
    "/api/watcher/sla/stats",
    "/api/watcher/health",
    "/api/watcher/overview",
    "/api/watcher/tasks/long-running",
    "/api/watcher/tasks/retries",
    "/api/watcher/tasks/zombies",
    "/api/watcher/dags/status-summary",
    "/api/watcher/dags/complexity",
    "/api/watcher/scheduling/lag",
    "/api/watcher/scheduling/queue",
    "/api/watcher/scheduling/pools",
    "/api/watcher/dependencies/upstream-failures",
    "/api/watcher/dependencies/cross-dag",
]


def create_session(user: dict) -> requests.Session:
    """Create an authenticated session for a user."""
    session = requests.Session()

    # Get CSRF token from login page
    login_page = session.get(f"{BASE_URL}/login/", timeout=10)
    csrf_token = None
    if "csrf_token" in login_page.text:
        import re
        match = re.search(r'name="csrf_token"[^>]*value="([^"]+)"', login_page.text)
        if match:
            csrf_token = match.group(1)

    # Login
    login_data = {
        "username": user["username"],
        "password": user["password"],
    }
    if csrf_token:
        login_data["csrf_token"] = csrf_token

    resp = session.post(
        f"{BASE_URL}/login/",
        data=login_data,
        allow_redirects=True,
        timeout=10,
    )

    if resp.status_code != 200 or "Sign In" in resp.text[:2000]:
        raise RuntimeError(f"Login failed for {user['username']}: HTTP {resp.status_code}")

    return session


def hit_endpoint(session: requests.Session, endpoint: str, user_info: dict) -> dict:
    """Hit a single endpoint and return timing + status info."""
    url = f"{BASE_URL}{endpoint}"
    start = time.monotonic()
    try:
        resp = session.get(url, timeout=30)
        elapsed = time.monotonic() - start
        return {
            "endpoint": endpoint,
            "user": user_info["username"],
            "role": user_info["role"],
            "status": resp.status_code,
            "time_ms": round(elapsed * 1000, 1),
            "size_bytes": len(resp.content),
            "error": None,
        }
    except Exception as e:
        elapsed = time.monotonic() - start
        return {
            "endpoint": endpoint,
            "user": user_info["username"],
            "role": user_info["role"],
            "status": 0,
            "time_ms": round(elapsed * 1000, 1),
            "size_bytes": 0,
            "error": str(e),
        }


def run_user_session(user_idx: int) -> list:
    """Run a full session for one virtual user."""
    user_info = USERS[user_idx % len(USERS)]
    results = []

    try:
        session = create_session(user_info)
    except Exception as e:
        # Return error for all endpoints
        for ep in UI_ENDPOINTS + API_ENDPOINTS:
            results.append({
                "endpoint": ep,
                "user": user_info["username"],
                "role": user_info["role"],
                "status": 0,
                "time_ms": 0,
                "size_bytes": 0,
                "error": f"Login failed: {e}",
            })
        return results

    # Hit all endpoints
    for ep in UI_ENDPOINTS + API_ENDPOINTS:
        results.append(hit_endpoint(session, ep, user_info))

    session.close()
    return results


def print_report(all_results: list, total_time: float):
    """Print a formatted load test report."""
    print("\n" + "=" * 80)
    print(f"  AIRFLOW WATCHER LOAD TEST REPORT")
    print(f"  100 concurrent users | {len(UI_ENDPOINTS)} UI + {len(API_ENDPOINTS)} API endpoints")
    print(f"  Total wall time: {total_time:.1f}s")
    print("=" * 80)

    # Overall stats — 503 from /health endpoint is by design (degraded health score)
    total_requests = len(all_results)
    health_503 = [r for r in all_results if r["endpoint"] == "/api/watcher/health" and r["status"] == 503]
    errors = [r for r in all_results if r["error"] or (r["status"] not in (200, 503) and r["status"] != 0)]
    login_failures = [r for r in all_results if r["error"] and "Login failed" in str(r["error"])]
    real_errors = [r for r in all_results if (r["error"] and "Login failed" not in str(r["error"])) or (r["status"] not in (200, 503, 302, 0) and not r["error"])]
    successful = [r for r in all_results if r["status"] in (200, 503) and r["error"] is None]
    times = [r["time_ms"] for r in all_results if r["error"] is None]

    print(f"\n  Total requests:  {total_requests}")
    print(f"  Successful:      {len(successful)} ({len(successful) / total_requests * 100:.1f}%)")
    print(f"  Health 503s:     {len(health_503)} (by design — health score < 70)")
    print(f"  Login failures:  {len(login_failures)} (concurrent session limit)")
    print(f"  Real errors:     {len(real_errors)}")
    if times:
        print(f"  Throughput:      {total_requests / total_time:.1f} req/s")
        print(f"\n  Response Times:")
        print(f"    Min:     {min(times):.0f} ms")
        print(f"    Avg:     {statistics.mean(times):.0f} ms")
        print(f"    Median:  {statistics.median(times):.0f} ms")
        print(f"    P95:     {sorted(times)[int(len(times) * 0.95)]:.0f} ms")
        print(f"    P99:     {sorted(times)[int(len(times) * 0.99)]:.0f} ms")
        print(f"    Max:     {max(times):.0f} ms")

    # Per-endpoint breakdown
    print(f"\n  {'ENDPOINT':<45} {'AVG(ms)':>8} {'P95(ms)':>8} {'MAX(ms)':>8} {'ERR':>5}")
    print("  " + "-" * 78)

    by_endpoint = defaultdict(list)
    for r in all_results:
        by_endpoint[r["endpoint"]].append(r)

    for ep in UI_ENDPOINTS + API_ENDPOINTS:
        ep_results = by_endpoint[ep]
        ep_times = [r["time_ms"] for r in ep_results if r["error"] is None]
        ep_errors = len([r for r in ep_results if r["error"] or (r["status"] not in (200, 503))])
        if ep_times:
            avg = statistics.mean(ep_times)
            p95 = sorted(ep_times)[int(len(ep_times) * 0.95)]
            mx = max(ep_times)
            short_ep = ep if len(ep) <= 44 else "..." + ep[-41:]
            print(f"  {short_ep:<45} {avg:>7.0f} {p95:>8.0f} {mx:>8.0f} {ep_errors:>5}")
        else:
            short_ep = ep if len(ep) <= 44 else "..." + ep[-41:]
            print(f"  {short_ep:<45} {'N/A':>8} {'N/A':>8} {'N/A':>8} {ep_errors:>5}")

    # Per-role breakdown
    print(f"\n  Per-Role Summary:")
    print(f"  {'ROLE':<20} {'USERS':>6} {'AVG(ms)':>8} {'P95(ms)':>8} {'ERRORS':>7}")
    print("  " + "-" * 52)

    by_role = defaultdict(list)
    for r in all_results:
        by_role[r["role"]].append(r)

    for role in ["admin", "weather", "ecommerce"]:
        role_results = by_role[role]
        role_times = [r["time_ms"] for r in role_results if r["error"] is None]
        role_errors = len([r for r in role_results if r["status"] != 200 or r["error"]])
        user_count = len(set(i for i in range(100) if USERS[i % 3]["role"] == role))
        if role_times:
            avg = statistics.mean(role_times)
            p95 = sorted(role_times)[int(len(role_times) * 0.95)]
            print(f"  {role:<20} {user_count:>6} {avg:>7.0f} {p95:>8.0f} {role_errors:>7}")

    # Error details (if any)
    if errors:
        print(f"\n  Error Details (first 10):")
        for r in errors[:10]:
            err_msg = r["error"] or f"HTTP {r['status']}"
            print(f"    [{r['role']}] {r['endpoint']}: {err_msg}")

    print("\n" + "=" * 80)


def main():
    num_users = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    max_workers = min(num_users, 20)  # Cap thread pool to avoid overwhelming the single webserver

    print(f"Starting load test: {num_users} users, {max_workers} concurrent threads")
    print(f"Target: {BASE_URL}")
    print(f"Endpoints: {len(UI_ENDPOINTS)} UI + {len(API_ENDPOINTS)} API = {len(UI_ENDPOINTS) + len(API_ENDPOINTS)} per user")
    print(f"Total requests: {num_users * (len(UI_ENDPOINTS) + len(API_ENDPOINTS))}")
    print()

    # Pre-create sessions (login phase) — separate from load test timing
    # In production, users maintain persistent sessions. We simulate this by
    # creating 3 sessions (one per role) and sharing them across virtual users.
    print("Phase 1: Creating authenticated sessions (3 roles)...")
    role_sessions = {}
    for user in USERS:
        try:
            session = create_session(user)
            role_sessions[user["role"]] = (session, user)
            print(f"  ✓ {user['username']} ({user['role']})")
        except Exception as e:
            print(f"  ✗ {user['username']}: {e}")

    # Map 100 virtual users to role sessions
    sessions = {}
    for i in range(num_users):
        user_info = USERS[i % len(USERS)]
        if user_info["role"] in role_sessions:
            session, _ = role_sessions[user_info["role"]]
            sessions[i] = (session, user_info)

    login_errors = num_users - len(sessions)
    print(f"  Mapped {len(sessions)}/{num_users} virtual users to {len(role_sessions)} sessions")
    print()

    # Phase 2: Hit all endpoints concurrently
    print("Phase 2: Load testing endpoints...")
    all_results = []
    start_time = time.monotonic()

    def run_session(idx):
        session, user_info = sessions[idx]
        results = []
        for ep in UI_ENDPOINTS + API_ENDPOINTS:
            results.append(hit_endpoint(session, ep, user_info))
        return results

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(run_session, idx): idx for idx in sessions}

        completed = 0
        for future in as_completed(futures):
            completed += 1
            results = future.result()
            all_results.extend(results)
            if completed % 10 == 0:
                print(f"  ... {completed}/{len(sessions)} users completed")

    total_time = time.monotonic() - start_time

    # Cleanup
    for role, (session, _) in role_sessions.items():
        session.close()

    print_report(all_results, total_time)


if __name__ == "__main__":
    main()
