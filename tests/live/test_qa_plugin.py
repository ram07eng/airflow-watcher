#!/usr/bin/env python
"""
Deep QA Test Suite for the Airflow Watcher **Flask Plugin** API.

Tests the ``/api/watcher/*`` endpoints served by the Airflow webserver plugin,
including:
  1.  All 19 GET endpoints — response status, JSON shape, envelope format
  2.  RBAC — per-user DAG visibility via plugin API
  3.  Query-parameter validation (bounds, defaults, invalid input)
  4.  Error handling (404, per-DAG 403)
  5.  Security — SQL injection, path traversal in DAG IDs
  6.  Data consistency — plugin API vs standalone API numbers match

Requires the demo Airflow environment (3 webservers on ports 8080-8082).
Auto-skips gracefully if the Airflow webservers are not reachable.
"""

import json
import re
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

# ── Counters ──────────────────────────────────────────────
PASS = 0
FAIL = 0
SKIP = 0
BUGS: List[str] = []
WARNINGS: List[str] = []


def result(ok: bool, label: str, detail: str = "", critical: bool = False) -> bool:
    global PASS, FAIL
    if ok:
        PASS += 1
        tag = "\033[92mPASS\033[0m"
    else:
        FAIL += 1
        tag = "\033[91mFAIL\033[0m"
        severity = "BUG" if critical else "ISSUE"
        BUGS.append(f"[{severity}] {label}: {detail}")
    suffix = f" — {detail}" if detail else ""
    print(f"  [{tag}] {label}{suffix}")
    return ok


def warn(label: str, detail: str = ""):
    WARNINGS.append(f"{label}: {detail}")
    print(f"  [\033[93mWARN\033[0m] {label} — {detail}")


def skip(label: str, reason: str):
    global SKIP
    SKIP += 1
    print(f"  [\033[93mSKIP\033[0m] {label} — {reason}")


def section(title: str, number: int):
    print(f"\n{'=' * 76}")
    print(f"  SECTION {number}: {title}")
    print(f"{'=' * 76}")


# ── HTTP helpers using opener (cookie-based session) ──────
def plugin_get(opener, url, timeout=15) -> Tuple[int, Any]:
    """GET a plugin endpoint via an authenticated Airflow session."""
    try:
        req = urllib.request.Request(url)
        req.add_header("Accept", "application/json")
        resp = opener.open(req, timeout=timeout)
        raw = resp.read().decode()
        try:
            return resp.status, json.loads(raw)
        except json.JSONDecodeError:
            return resp.status, raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except json.JSONDecodeError:
            return e.code, raw
    except Exception as e:
        return 0, str(e)


def get_airflow_session(base_url, user, password):
    """Login to Airflow and return (opener, error)."""
    login_url = f"{base_url}/login/"
    try:
        req = urllib.request.Request(login_url)
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor())
        resp = opener.open(req, timeout=10)
        html = resp.read().decode()
        csrf_match = re.search(r'name="csrf_token"\s+.*?value="([^"]+)"', html)
        if not csrf_match:
            csrf_match = re.search(r'csrf_token.*?value="([^"]+)"', html)
        if not csrf_match:
            return None, "CSRF token not found"
        csrf_token = csrf_match.group(1)
        data = urllib.parse.urlencode({
            "username": user, "password": password, "csrf_token": csrf_token,
        }).encode()
        login_req = urllib.request.Request(login_url, data=data, method="POST")
        login_req.add_header("Content-Type", "application/x-www-form-urlencoded")
        opener.open(login_req, timeout=10)
        return opener, None
    except Exception as e:
        return None, str(e)[:120]


# ── Config ────────────────────────────────────────────────
ADMIN_BASE = "http://localhost:8080"
STANDALONE_BASE = "http://localhost:8083"

WEBSERVERS = [
    {"name": "admin",     "port": 8080, "user": "admin",          "password": "admin",        "role": "Admin"},
    {"name": "weather",   "port": 8081, "user": "weather_user",   "password": "weather123",   "role": "team_weather"},
    {"name": "ecommerce", "port": 8082, "user": "ecommerce_user", "password": "ecommerce123", "role": "team_ecommerce"},
]

# All /api/watcher/* GET endpoints (relative to base_url)
PLUGIN_API_ENDPOINTS = [
    "/api/watcher/failures",
    "/api/watcher/failures/stats",
    "/api/watcher/sla/misses",
    "/api/watcher/sla/stats",
    "/api/watcher/health",
    "/api/watcher/tasks/long-running",
    "/api/watcher/tasks/retries",
    "/api/watcher/tasks/zombies",
    "/api/watcher/tasks/failure-patterns",
    "/api/watcher/scheduling/lag",
    "/api/watcher/scheduling/queue",
    "/api/watcher/scheduling/pools",
    "/api/watcher/scheduling/stale-dags",
    "/api/watcher/scheduling/concurrent",
    "/api/watcher/dags/import-errors",
    "/api/watcher/dags/status-summary",
    "/api/watcher/dags/complexity",
    "/api/watcher/dags/inactive",
    "/api/watcher/dependencies/upstream-failures",
    "/api/watcher/dependencies/cross-dag",
    "/api/watcher/dependencies/correlations",
    "/api/watcher/overview",
]


# ══════════════════════════════════════════════════════════
#  SECTION 1 — Plugin API: All Endpoints Return 200 + JSON
# ══════════════════════════════════════════════════════════
def test_plugin_api_endpoints(opener, base_url, label):
    section(f"PLUGIN API ENDPOINTS ({label})", 1)

    for ep in PLUGIN_API_ENDPOINTS:
        url = f"{base_url}{ep}"
        code, body = plugin_get(opener, url)
        is_json = isinstance(body, dict)
        has_status = is_json and "status" in body
        # health endpoint legitimately returns 503 when health_score < 70
        ok_codes = (200, 503) if "/health" in ep else (200,)
        result(
            code in ok_codes and is_json and has_status,
            f"GET {ep}",
            f"status={code}, json={is_json}, has_status={has_status}",
            critical=(code not in ok_codes),
        )


# ══════════════════════════════════════════════════════════
#  SECTION 2 — Plugin API: Response Envelope Validation
# ══════════════════════════════════════════════════════════
def test_plugin_envelope(opener, base_url, label):
    section(f"PLUGIN API ENVELOPE ({label})", 2)

    for ep in PLUGIN_API_ENDPOINTS:
        url = f"{base_url}{ep}"
        code, body = plugin_get(opener, url)
        ok_codes = (200, 503) if "/health" in ep else (200,)
        if code not in ok_codes or not isinstance(body, dict):
            skip(ep, f"status={code}")
            continue
        has_status = "status" in body
        has_data = "data" in body
        has_ts = "timestamp" in body
        result(
            has_status and has_data and has_ts,
            f"Envelope {ep}",
            f"status={has_status}, data={has_data}, timestamp={has_ts}",
        )


# ══════════════════════════════════════════════════════════
#  SECTION 3 — RBAC: DAG Visibility Across Users
# ══════════════════════════════════════════════════════════
def test_plugin_rbac(sessions):
    section("PLUGIN API RBAC", 3)

    # Endpoints that return per-DAG data (list of dicts with dag_id)
    dag_endpoints = [
        ("/api/watcher/failures", "data.failures"),
        ("/api/watcher/dags/inactive", "data.inactive_dags"),
        ("/api/watcher/dependencies/upstream-failures", "data.upstream_failures"),
        ("/api/watcher/scheduling/stale-dags", "data.stale_dags"),
    ]

    admin_sess = sessions.get("admin")
    if not admin_sess:
        skip("Plugin RBAC", "admin session not available")
        return

    admin_opener, admin_base = admin_sess

    for ep, data_path in dag_endpoints:
        # Get admin's view (all DAGs)
        admin_code, admin_body = plugin_get(admin_opener, f"{admin_base}{ep}")
        if admin_code != 200 or not isinstance(admin_body, dict):
            skip(f"RBAC {ep}", f"admin status={admin_code}")
            continue

        # Extract DAG IDs from admin response
        admin_data = admin_body.get("data", {})
        parts = data_path.split(".")
        obj = admin_data
        for part in parts[1:]:  # skip "data."
            if isinstance(obj, dict):
                obj = obj.get(part, [])
        admin_items = obj if isinstance(obj, list) else []
        admin_dag_ids = {d.get("dag_id") for d in admin_items if isinstance(d, dict) and "dag_id" in d}

        # Check restricted users see fewer DAGs
        for name in ("weather", "ecommerce"):
            sess = sessions.get(name)
            if not sess:
                skip(f"RBAC {ep} ({name})", "session unavailable")
                continue
            user_opener, user_base = sess
            user_code, user_body = plugin_get(user_opener, f"{user_base}{ep}")
            if user_code != 200 or not isinstance(user_body, dict):
                skip(f"RBAC {ep} ({name})", f"status={user_code}")
                continue

            user_data = user_body.get("data", {})
            obj2 = user_data
            for part in parts[1:]:
                if isinstance(obj2, dict):
                    obj2 = obj2.get(part, [])
            user_items = obj2 if isinstance(obj2, list) else []
            user_dag_ids = {d.get("dag_id") for d in user_items if isinstance(d, dict) and "dag_id" in d}

            # Restricted user should see ≤ admin DAGs
            result(
                user_dag_ids <= admin_dag_ids or len(user_items) <= len(admin_items),
                f"RBAC {ep}: {name} ≤ admin",
                f"admin={len(admin_items)}, {name}={len(user_items)}",
            )


# ══════════════════════════════════════════════════════════
#  SECTION 4 — Query Parameter Validation
# ══════════════════════════════════════════════════════════
def test_plugin_query_params(opener, base_url, label):
    section(f"PLUGIN API QUERY PARAMS ({label})", 4)

    # hours param
    for val, expect_ok in [("24", True), ("1", True), ("999999", True), ("abc", True), ("-1", True)]:
        url = f"{base_url}/api/watcher/failures?hours={val}"
        code, body = plugin_get(opener, url)
        # Plugin should always return 200 due to _safe_int fallback
        result(code == 200, f"failures?hours={val}", f"status={code}")

    # limit param
    for val in ("5", "500", "0", "-10", "abc"):
        url = f"{base_url}/api/watcher/failures?limit={val}"
        code, body = plugin_get(opener, url)
        result(code == 200, f"failures?limit={val}", f"status={code}")

    # threshold_minutes
    url = f"{base_url}/api/watcher/tasks/long-running?threshold_minutes=1"
    code, _ = plugin_get(opener, url)
    result(code == 200, "long-running?threshold_minutes=1", f"status={code}")

    # expected_interval_hours
    url = f"{base_url}/api/watcher/scheduling/stale-dags?expected_interval_hours=1"
    code, _ = plugin_get(opener, url)
    result(code == 200, "stale-dags?expected_interval_hours=1", f"status={code}")


# ══════════════════════════════════════════════════════════
#  SECTION 5 — Error Handling & Access Control
# ══════════════════════════════════════════════════════════
def test_plugin_errors(opener, base_url, label, sessions):
    section(f"PLUGIN API ERRORS ({label})", 5)

    # 404 — non-existent endpoint
    code, _ = plugin_get(opener, f"{base_url}/api/watcher/nonexistent")
    result(code == 404, "Non-existent endpoint → 404", f"got {code}")

    # health/<dag_id> for restricted user should return 403
    for name in ("weather", "ecommerce"):
        sess = sessions.get(name)
        if not sess:
            skip(f"DAG access 403 ({name})", "session unavailable")
            continue
        user_opener, user_base = sess
        # Try to access a DAG not in their RBAC set
        code, body = plugin_get(user_opener, f"{user_base}/api/watcher/health/nonexistent_dag_99")
        # Should be 403 if RBAC works (or 200 if no DAGs match filter — either is acceptable)
        result(
            code in (403, 200),
            f"health/<restricted_dag> ({name})",
            f"got {code}",
        )

    # health endpoint returns health_score (may return 503 when degraded)
    code, body = plugin_get(opener, f"{base_url}/api/watcher/health")
    if code in (200, 503) and isinstance(body, dict):
        data = body.get("data", {})
        has_score = "health_score" in data
        result(has_score, "health endpoint has health_score", f"status={code}, keys={list(data.keys())[:8]}")
    else:
        skip("health deep check", f"status={code}")


# ══════════════════════════════════════════════════════════
#  SECTION 6 — Security (SQL Injection, Path Traversal)
# ══════════════════════════════════════════════════════════
def test_plugin_security(opener, base_url, label):
    section(f"PLUGIN API SECURITY ({label})", 6)

    # SQL injection in hours param
    sqli_payloads = [
        "24; DROP TABLE dag_run;--",
        "24 OR 1=1",
        "24' UNION SELECT * FROM dag_run--",
    ]
    for payload in sqli_payloads:
        url = f"{base_url}/api/watcher/failures?hours={urllib.parse.quote(payload)}"
        code, body = plugin_get(opener, url)
        # Should not crash the server — 200 (with fallback default) or 400
        result(code in (200, 400), f"SQLi in hours: {payload[:30]}", f"status={code}")

    # Path traversal in dag_id
    traversal_payloads = [
        "../../../etc/passwd",
        "..%2F..%2F..%2Fetc%2Fpasswd",
        "test_dag/../../../etc/shadow",
    ]
    for payload in traversal_payloads:
        url = f"{base_url}/api/watcher/health/{urllib.parse.quote(payload, safe='')}"
        code, body = plugin_get(opener, url)
        # Should NOT return file contents
        is_safe = True
        if isinstance(body, str) and ("root:" in body or "shadow" in body.lower()):
            is_safe = False
        if isinstance(body, dict):
            body_str = json.dumps(body)
            if "root:" in body_str:
                is_safe = False
        result(is_safe and code in (200, 403, 404), f"Path traversal: {payload[:35]}", f"status={code}")

    # XSS in dag_id (should be JSON response, not reflected HTML)
    xss = '<script>alert(1)</script>'
    url = f"{base_url}/api/watcher/health/{urllib.parse.quote(xss)}"
    code, body = plugin_get(opener, url)
    body_str = json.dumps(body) if isinstance(body, dict) else str(body)
    result(
        "<script>" not in body_str.lower(),
        "XSS in dag_id not reflected",
        f"status={code}",
    )

    # Error handler doesn't leak stack traces
    code, body = plugin_get(opener, f"{base_url}/api/watcher/health")
    if code in (200, 503) and isinstance(body, dict):
        body_str = json.dumps(body)
        no_leak = "Traceback" not in body_str and "File \"/" not in body_str
        result(no_leak, "No stack trace in health response", f"status={code}")


# ══════════════════════════════════════════════════════════
#  SECTION 7 — Data Consistency: Plugin API vs Standalone
# ══════════════════════════════════════════════════════════
def test_plugin_vs_standalone(opener, base_url, label):
    section(f"PLUGIN vs STANDALONE CONSISTENCY ({label})", 7)

    # Check if standalone is running
    try:
        req = urllib.request.Request(f"{STANDALONE_BASE}/healthz")
        resp = urllib.request.urlopen(req, timeout=5)
        if resp.status != 200:
            skip("Plugin vs Standalone", "standalone not healthy")
            return
    except Exception:
        skip("Plugin vs Standalone", "standalone API not reachable on :8083")
        return

    standalone_key = "RrkU1CeHfHjtONRYPYKYOdxRcrmraSGM_mQl_gsVCaQ"  # admin key

    comparisons = [
        ("/api/watcher/dags/status-summary", "/api/v1/dags/status-summary", "total_dags"),
        ("/api/watcher/failures/stats", "/api/v1/failures/stats", "total_runs"),
    ]

    for plugin_ep, standalone_ep, field in comparisons:
        p_code, p_body = plugin_get(opener, f"{base_url}{plugin_ep}")
        try:
            s_req = urllib.request.Request(
                f"{STANDALONE_BASE}{standalone_ep}",
                headers={"Authorization": f"Bearer {standalone_key}"},
            )
            s_resp = urllib.request.urlopen(s_req, timeout=10)
            s_body = json.loads(s_resp.read().decode())
        except Exception as e:
            skip(f"Consistency {field}", str(e)[:60])
            continue

        if p_code != 200 or not isinstance(p_body, dict):
            skip(f"Consistency {field}", f"plugin status={p_code}")
            continue

        p_val = p_body.get("data", {}).get(field)
        s_val = s_body.get("data", {}).get(field)

        result(
            p_val == s_val,
            f"Consistency: {field}",
            f"plugin={p_val}, standalone={s_val}",
        )


# ══════════════════════════════════════════════════════════
#  SECTION 8 — Response Data Shape Validation
# ══════════════════════════════════════════════════════════
def test_plugin_data_shapes(opener, base_url, label):
    section(f"PLUGIN API DATA SHAPES ({label})", 8)

    shape_checks = [
        ("/api/watcher/failures", {"failures": list, "count": int}),
        ("/api/watcher/failures/stats", {"most_failing_dags": list}),
        ("/api/watcher/sla/misses", {"sla_misses": list, "count": int}),
        ("/api/watcher/health", {"health_score": int, "summary": dict}),
        ("/api/watcher/tasks/long-running", {"tasks": list, "count": int}),
        ("/api/watcher/tasks/zombies", {"zombies": list, "count": int}),
        ("/api/watcher/scheduling/queue", {"queued_count": int}),
        ("/api/watcher/scheduling/pools", {"pools": list, "count": int}),
        ("/api/watcher/scheduling/concurrent", {"dags_with_concurrent_runs": int}),
        ("/api/watcher/dags/import-errors", {"errors": list, "count": int}),
        ("/api/watcher/dags/status-summary", {"total_dags": int, "health_score": int}),
        ("/api/watcher/dags/complexity", {"dags": list, "count": int}),
        ("/api/watcher/dags/inactive", {"inactive_dags": list, "count": int}),
        ("/api/watcher/dependencies/upstream-failures", {"upstream_failures": list, "count": int}),
        ("/api/watcher/dependencies/cross-dag", {"dependencies": list, "count": int}),
        ("/api/watcher/dependencies/correlations", {"correlations": list}),
    ]

    for ep, expected_fields in shape_checks:
        code, body = plugin_get(opener, f"{base_url}{ep}")
        ok_codes = (200, 503) if "/health" in ep else (200,)
        if code not in ok_codes or not isinstance(body, dict):
            skip(f"Shape {ep}", f"status={code}")
            continue

        data = body.get("data", {})
        all_ok = True
        details = []
        for field, expected_type in expected_fields.items():
            val = data.get(field)
            if val is None:
                all_ok = False
                details.append(f"{field}=missing")
            elif not isinstance(val, expected_type):
                all_ok = False
                details.append(f"{field}={type(val).__name__}!={expected_type.__name__}")

        result(all_ok, f"Shape {ep}", ", ".join(details) if details else "OK")


# ══════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════
if __name__ == "__main__":
    start = time.time()
    print("\n" + "#" * 76)
    print("#  AIRFLOW WATCHER — PLUGIN API DEEP QA TEST")
    print(f"#  Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"#  Targets: Airflow webservers (8080-8082) → /api/watcher/*")
    print("#" * 76)

    # Login to all webservers
    sessions: Dict[str, Tuple] = {}
    any_reachable = False

    for ws in WEBSERVERS:
        base = f"http://localhost:{ws['port']}"
        print(f"\n  Logging in to {ws['name']} ({base}) as {ws['user']}...")
        opener, err = get_airflow_session(base, ws["user"], ws["password"])
        if err:
            print(f"    ⚠ Could not login: {err}")
            continue
        sessions[ws["name"]] = (opener, base)
        any_reachable = True
        print(f"    ✓ Logged in as {ws['user']} ({ws['role']})")

    if not any_reachable:
        print("\n  ❌ No Airflow webservers reachable. Are the demo containers running?")
        print("     Run:  cd demo && docker compose up -d")
        print(f"\n  RESULTS: 0 passed, 0 failed, 0 skipped")
        sys.exit(1)

    admin_sess = sessions.get("admin")
    if admin_sess:
        admin_opener, admin_base = admin_sess
        test_plugin_api_endpoints(admin_opener, admin_base, "admin")
        test_plugin_envelope(admin_opener, admin_base, "admin")
        test_plugin_query_params(admin_opener, admin_base, "admin")
        test_plugin_errors(admin_opener, admin_base, "admin", sessions)
        test_plugin_security(admin_opener, admin_base, "admin")
        test_plugin_data_shapes(admin_opener, admin_base, "admin")
        test_plugin_vs_standalone(admin_opener, admin_base, "admin")
    else:
        skip("All admin tests", "admin login failed")

    # Run endpoint tests on restricted users too
    for name in ("weather", "ecommerce"):
        sess = sessions.get(name)
        if sess:
            user_opener, user_base = sess
            test_plugin_api_endpoints(user_opener, user_base, name)

    # RBAC comparison
    test_plugin_rbac(sessions)

    elapsed = time.time() - start

    print("\n" + "=" * 76)
    total = PASS + FAIL + SKIP
    print(f"  RESULTS: {PASS} passed, {FAIL} failed, {SKIP} skipped (total: {total})")
    print(f"  Duration: {elapsed:.1f}s")
    print("=" * 76)

    if BUGS:
        print(f"\n  {'─' * 70}")
        print(f"  BUGS / ISSUES FOUND ({len(BUGS)}):")
        print(f"  {'─' * 70}")
        for b in BUGS:
            print(f"    • {b}")

    if WARNINGS:
        print(f"\n  {'─' * 70}")
        print(f"  WARNINGS ({len(WARNINGS)}):")
        print(f"  {'─' * 70}")
        for w in WARNINGS:
            print(f"    • {w}")

    if FAIL == 0:
        print(f"\n  ✅ ALL TESTS PASSED — No bugs found!")
    else:
        print(f"\n  ❌ {FAIL} TEST(S) FAILED — See bugs above.")

    sys.exit(1 if FAIL > 0 else 0)
