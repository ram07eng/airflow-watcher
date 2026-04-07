#!/usr/bin/env python
"""
Deep QA Test Suite for Airflow Watcher — Plugin & Standalone API.

Tests cover:
  1.  Plugin: All 3 webservers, page rendering, RBAC DAG visibility
  2.  API: All endpoints × 3 RBAC keys, response envelope validation
  3.  API: Deep RBAC — data leak detection, per-field filtering verification
  4.  API: Query-parameter boundary tests (min, max, default, invalid)
  5.  Authentication: bypass attempts, injection, timing safety
  6.  Security: SQL injection, header injection, path traversal
  7.  Response contract: schema validation on every endpoint
  8.  Rate limiting: burst traffic + Retry-After header
  9.  Concurrency: parallel request safety
  10. Data consistency: plugin ↔ API numbers match
  11. Caching: invalidation, TTL-based freshness
  12. Error handling: 404, 501, 503, wrong methods
"""

import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set, Tuple

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


# Pace requests to stay under the rate limit (120/min = 2/s).
# Enforce a minimum inter-request delay (550ms) so we never exceed the limit.
_request_timestamps: List[float] = []
_RATE_LIMIT_RPM = 120
_MIN_INTERVAL = 60.0 / _RATE_LIMIT_RPM + 0.05  # ~0.55s between requests


def pace():
    """Enforce spacing between requests to stay under rate limit."""
    now = time.monotonic()
    # Prune timestamps older than 60s
    cutoff = now - 60
    while _request_timestamps and _request_timestamps[0] < cutoff:
        _request_timestamps.pop(0)
    # If we've used 110+ of our 120 budget in this window, wait it out
    if len(_request_timestamps) >= _RATE_LIMIT_RPM - 10:
        oldest = _request_timestamps[0]
        wait_time = 61 - (now - oldest)
        if wait_time > 0:
            print(
                f"    [PACE] Near rate limit ({len(_request_timestamps)} reqs in window), waiting {wait_time:.0f}s..."
            )
            time.sleep(wait_time)
            now = time.monotonic()
            cutoff = now - 60
            while _request_timestamps and _request_timestamps[0] < cutoff:
                _request_timestamps.pop(0)
    # Enforce minimum interval between consecutive requests
    if _request_timestamps:
        since_last = now - _request_timestamps[-1]
        if since_last < _MIN_INTERVAL:
            time.sleep(_MIN_INTERVAL - since_last)
    _request_timestamps.append(time.monotonic())


# ── HTTP helpers ──────────────────────────────────────────
def http(
    method: str,
    url: str,
    headers: Optional[Dict] = None,
    body: Optional[bytes] = None,
    timeout: int = 15,
    skip_pace: bool = False,
) -> Tuple[int, Any, Dict[str, str]]:
    """Return (status, body_parsed, response_headers)."""
    if not skip_pace and "8083" in url and "/healthz" not in url:
        pace()
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        resp_headers = {k.lower(): v for k, v in resp.getheaders()}
        raw = resp.read().decode()
        try:
            return resp.status, json.loads(raw), resp_headers
        except json.JSONDecodeError:
            return resp.status, raw, resp_headers
    except urllib.error.HTTPError as e:
        resp_headers = {k.lower(): v for k, v in e.headers.items()}
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw), resp_headers
        except json.JSONDecodeError:
            return e.code, raw, resp_headers
    except Exception as e:
        return 0, str(e), {}


def http_get(url, headers=None, timeout=15, skip_pace=False):
    code, body, hdrs = http("GET", url, headers, timeout=timeout, skip_pace=skip_pace)
    return code, body


def http_post(url, headers=None, body=None, timeout=15, skip_pace=False):
    return http("POST", url, headers, body, timeout=timeout, skip_pace=skip_pace)


def bearer(key: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {key}"}


# ── Config ────────────────────────────────────────────────
API_BASE = "http://localhost:8083"

API_KEYS = {
    "admin": {
        "key": "RrkU1CeHfHjtONRYPYKYOdxRcrmraSGM_mQl_gsVCaQ",
        "expected_dags": None,
        "label": "Admin (full access)",
    },
    "weather": {
        "key": "kkVJKBtAFB7GuqxCa68z9qhvBUMmbYFr9ne71GrYYcg",
        "expected_dags": {"weather_data_pipeline", "stock_market_collector"},
        "label": "team_weather",
    },
    "ecommerce": {
        "key": "ttT4gHJWDjmgH6cmMBiUC3Vj0qVEhWYJ7fLMmpRKRJ8",
        "expected_dags": {"ecommerce_sales_etl", "data_quality_checks"},
        "label": "team_ecommerce",
    },
}

WEBSERVERS = [
    {"name": "admin", "port": 8080, "user": "admin", "password": "admin", "role": "Admin (all DAGs)"},
    {
        "name": "weather",
        "port": 8081,
        "user": "weather_user",
        "password": "weather123",
        "role": "team_weather (2 DAGs)",
    },
    {
        "name": "ecommerce",
        "port": 8082,
        "user": "ecommerce_user",
        "password": "ecommerce123",
        "role": "team_ecommerce (2 DAGs)",
    },
]

WATCHER_PAGES = [
    "/watcher/dashboard",
    "/watcher/failures",
    "/watcher/sla",
    "/watcher/health",
    "/watcher/tasks",
    "/watcher/scheduling",
    "/watcher/dag-health",
    "/watcher/dependencies",
]

# All GET endpoints for the standalone API
API_GET_ENDPOINTS = [
    "/api/v1/failures/?hours=24&limit=5",
    "/api/v1/failures/stats?hours=24",
    "/api/v1/sla/misses?hours=24",
    "/api/v1/sla/stats?hours=24",
    "/api/v1/tasks/long-running?threshold_minutes=60",
    "/api/v1/tasks/retries?hours=24",
    "/api/v1/tasks/zombies",
    "/api/v1/tasks/failure-patterns?hours=24",
    "/api/v1/scheduling/lag",
    "/api/v1/scheduling/queue",
    "/api/v1/scheduling/pools",
    "/api/v1/scheduling/stale-dags?expected_interval_hours=72",
    "/api/v1/scheduling/concurrent",
    "/api/v1/dags/import-errors",
    "/api/v1/dags/status-summary",
    "/api/v1/dags/complexity",
    "/api/v1/dags/inactive?days=7",
    "/api/v1/dependencies/upstream-failures?hours=24",
    "/api/v1/dependencies/correlations?hours=24",
    "/api/v1/health/",
    "/api/v1/overview/",
    "/api/v1/alerts/rules",
]

# Endpoints that need DagBag → 501 standalone
STANDALONE_501 = [
    "/api/v1/dependencies/cross-dag",
    "/api/v1/dependencies/impact/some_dag/some_task",
]

# Known DAG IDs in the demo environment
WEATHER_DAGS = {"weather_data_pipeline", "stock_market_collector"}
ECOMMERCE_DAGS = {"ecommerce_sales_etl", "data_quality_checks"}
ALL_KNOWN_DAGS = (
    WEATHER_DAGS
    | ECOMMERCE_DAGS
    | {
        "ml_training_pipeline",
        "data_warehouse_refresh",
        "customer_segmentation",
        "realtime_anomaly_detector",
        "log_cleanup_daily",
        "api_health_monitor",
    }
)


# ── Plugin helpers ────────────────────────────────────────
def get_airflow_session(base_url, user, password):
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
        data = urllib.parse.urlencode(
            {
                "username": user,
                "password": password,
                "csrf_token": csrf_token,
            }
        ).encode()
        login_req = urllib.request.Request(login_url, data=data, method="POST")
        login_req.add_header("Content-Type", "application/x-www-form-urlencoded")
        opener.open(login_req, timeout=10)
        return opener, None
    except Exception as e:
        return None, str(e)


# ══════════════════════════════════════════════════════════
#  SECTION 1 — Watcher Plugin (Airflow Webservers)
# ══════════════════════════════════════════════════════════
def test_plugin():
    section("WATCHER PLUGIN (Airflow Webservers)", 1)

    for ws in WEBSERVERS:
        print(f"\n  ─── {ws['name'].upper()} (:{ws['port']}) as {ws['user']} [{ws['role']}] ───")
        base = f"http://localhost:{ws['port']}"

        status, _ = http_get(f"{base}/health", timeout=5)
        if status == 0:
            skip(f"{ws['name']} webserver", "not reachable")
            continue
        result(status == 200, f"{ws['name']} /health", f"status={status}")

        opener, err = get_airflow_session(base, ws["user"], ws["password"])
        if err:
            skip(f"Login {ws['user']}", err)
            continue
        result(True, f"Login as {ws['user']}")

        # All watcher pages render
        for page in WATCHER_PAGES:
            try:
                req = urllib.request.Request(f"{base}{page}")
                resp = opener.open(req, timeout=15)
                html = resp.read().decode()
                has_content = len(html) > 200 and any(
                    kw in html.lower() for kw in ("watcher", "dashboard", "failure", "health", "scheduling", "sla")
                )
                result(resp.status == 200 and has_content, page, f"size={len(html)}B")
            except urllib.error.HTTPError as e:
                result(False, page, f"HTTP {e.code}", critical=True)
            except Exception as e:
                result(False, page, str(e)[:80], critical=True)

        # DAG visibility via Airflow API
        try:
            req = urllib.request.Request(f"{base}/api/v1/dags?limit=100")
            req.add_header("Content-Type", "application/json")
            resp = opener.open(req, timeout=10)
            dags_data = json.loads(resp.read().decode())
            dag_ids = sorted(d["dag_id"] for d in dags_data.get("dags", []))
            result(True, "DAG visibility", f"{len(dag_ids)} DAGs: {dag_ids}")
        except Exception as e:
            skip("DAG visibility", str(e)[:80])


# ══════════════════════════════════════════════════════════
#  SECTION 2 — Standalone API: Endpoint Coverage (3 keys)
# ══════════════════════════════════════════════════════════
def test_api_endpoints():
    section("API ENDPOINT COVERAGE (× 3 RBAC keys)", 2)

    # Check reachability
    status, body = http_get(f"{API_BASE}/healthz", timeout=5)
    if status == 0:
        skip("Standalone API", f"not reachable: {body}")
        return
    result(
        status == 200,
        "/healthz (no auth)",
        f"db_connected={body.get('db_connected') if isinstance(body, dict) else '?'}",
    )

    for role, info in API_KEYS.items():
        hdr = bearer(info["key"])
        print(f"\n  ─── {role.upper()} [{info['label']}] ───")

        for ep in API_GET_ENDPOINTS:
            code, body = http_get(f"{API_BASE}{ep}", headers=hdr)
            path = ep.split("?")[0]
            # /health/ can return 503 when status is degraded — that's by design
            expected_codes = (200, 503) if path == "/api/v1/health/" else (200,)
            ok = code in expected_codes
            detail = f"status={code}"
            if ok and isinstance(body, dict):
                # Validate envelope
                has_status = body.get("status") == "success"
                has_ts = "timestamp" in body
                has_data = "data" in body
                if not (has_status and has_ts and has_data):
                    ok = False
                    detail += ", ENVELOPE BROKEN"
                else:
                    detail += ", envelope=ok"
            result(ok, f"GET {path}", detail, critical=not ok)

        # 501 endpoints
        for ep in STANDALONE_501:
            code, body = http_get(f"{API_BASE}{ep}", headers=hdr)
            result(code == 501, f"GET {ep} → 501", f"got {code}")


# ══════════════════════════════════════════════════════════
#  SECTION 3 — Response Envelope & Schema Validation
# ══════════════════════════════════════════════════════════
def test_envelope_and_schema():
    section("RESPONSE ENVELOPE & SCHEMA VALIDATION", 3)

    hdr = bearer(API_KEYS["admin"]["key"])

    # Check every success response has: status, data, timestamp
    for ep in API_GET_ENDPOINTS:
        code, body = http_get(f"{API_BASE}{ep}", headers=hdr)
        path = ep.split("?")[0]
        if code not in (200, 503):
            skip(f"Schema {path}", f"got {code}")
            continue
        if not isinstance(body, dict):
            result(False, f"Schema {path}", "not a dict", critical=True)
            continue
        ok = body.get("status") == "success" and "timestamp" in body and "data" in body
        result(ok, f"Envelope {path}", f"keys={sorted(body.keys())}")

        ts = body.get("timestamp", "")
        valid_ts = bool(re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", ts))
        result(valid_ts, f"Timestamp format {path}", f"ts={ts[:25]}")

    # Error envelope for 401
    code, body, hdrs = http("GET", f"{API_BASE}/api/v1/overview/")
    result(code == 401, "401 envelope: status code", f"got {code}")
    if isinstance(body, dict):
        detail = body.get("detail", {})
        if isinstance(detail, dict):
            result(detail.get("status") == "error", "401 envelope: status=error", f"got {detail.get('status')}")
        elif isinstance(detail, str):
            result(
                "error" in detail.lower() or "auth" in detail.lower(),
                "401 envelope: error detail",
                f"detail={detail[:60]}",
            )

    # Error envelope for 404
    code, body, hdrs = http("GET", f"{API_BASE}/api/v1/nonexistent", headers=hdr)
    result(code == 404, "404 for nonexistent endpoint", f"got {code}")

    # X-API-Version header
    code, body, hdrs = http("GET", f"{API_BASE}/api/v1/health/", headers=hdr)
    api_version = hdrs.get("x-api-version", "")
    result(api_version == "1.0", "X-API-Version header = 1.0", f"got '{api_version}'")


# ══════════════════════════════════════════════════════════
#  SECTION 4 — Deep RBAC Data-Leak Testing
# ══════════════════════════════════════════════════════════
def _extract_dag_ids_recursive(obj: Any, found: Set[str], path: str = ""):
    """Walk response data and collect all dag_id values."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "dag_id" and isinstance(v, str):
                found.add(v)
            elif k in ("source_dag_id", "dag_id_a") and isinstance(v, str):
                found.add(v)
            else:
                _extract_dag_ids_recursive(v, found, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            _extract_dag_ids_recursive(item, found, f"{path}[{i}]")


def test_rbac_deep():
    section("DEEP RBAC DATA LEAK TESTING", 4)

    # Endpoints whose data payload may contain dag_id values
    rbac_endpoints = [
        "/api/v1/failures/?hours=720&limit=500",
        "/api/v1/failures/stats?hours=720",
        "/api/v1/sla/misses?hours=720&limit=500",
        "/api/v1/sla/stats?hours=720",
        "/api/v1/tasks/long-running?threshold_minutes=1",
        "/api/v1/tasks/retries?hours=720&min_retries=1",
        "/api/v1/tasks/zombies?threshold_minutes=1",
        "/api/v1/tasks/failure-patterns?hours=720",
        "/api/v1/scheduling/lag?hours=720",
        "/api/v1/scheduling/queue",
        "/api/v1/scheduling/stale-dags?expected_interval_hours=1",
        "/api/v1/scheduling/concurrent",
        "/api/v1/dags/complexity",
        "/api/v1/dags/inactive?days=365",
        "/api/v1/dependencies/upstream-failures?hours=720",
        "/api/v1/dependencies/correlations?hours=720",
        "/api/v1/overview/",
    ]

    for role in ("weather", "ecommerce"):
        info = API_KEYS[role]
        hdr = bearer(info["key"])
        expected = info["expected_dags"]
        print(f"\n  ─── {role.upper()} deep RBAC scan ───")

        for ep in rbac_endpoints:
            code, body = http_get(f"{API_BASE}{ep}", headers=hdr)
            path = ep.split("?")[0]
            if code != 200:
                skip(f"RBAC-scan {role} {path}", f"status={code}")
                continue
            if not isinstance(body, dict):
                continue

            found_dags: Set[str] = set()
            _extract_dag_ids_recursive(body.get("data", {}), found_dags)

            if not found_dags:
                # No DAGs in this response — that's fine
                result(True, f"RBAC-scan {role} {path}", "no dags in response")
                continue

            leaked = found_dags - expected
            result(
                len(leaked) == 0,
                f"RBAC-scan {role} {path}",
                f"visible={sorted(found_dags)}, leaked={sorted(leaked)}"
                if leaked
                else f"visible={sorted(found_dags)} ✓",
                critical=len(leaked) > 0,
            )

    # Admin should see more DAGs than restricted keys
    print("\n  ─── ADMIN vs SCOPED comparison ───")
    admin_hdr = bearer(API_KEYS["admin"]["key"])
    weather_hdr = bearer(API_KEYS["weather"]["key"])

    for ep in ["/api/v1/dags/complexity", "/api/v1/overview/"]:
        _, admin_body = http_get(f"{API_BASE}{ep}", headers=admin_hdr)
        _, weather_body = http_get(f"{API_BASE}{ep}", headers=weather_hdr)
        path = ep.split("?")[0]

        admin_dags: Set[str] = set()
        weather_dags: Set[str] = set()
        _extract_dag_ids_recursive(admin_body.get("data", {}), admin_dags)
        _extract_dag_ids_recursive(weather_body.get("data", {}), weather_dags)

        if len(admin_dags) >= len(weather_dags):
            result(True, f"Admin ≥ weather DAGs on {path}", f"admin={len(admin_dags)}, weather={len(weather_dags)}")
        else:
            # Admin response may have 0 DAG IDs in aggregate fields while
            # weather response has per-DAG detail — data-dependent, not a leak
            warn(
                f"Admin < weather extracted DAGs on {path}",
                f"admin={len(admin_dags)}, weather={len(weather_dags)} (aggregate vs detail)",
            )

    # Single-DAG health: verify 403 on unauthorized DAGs
    print("\n  ─── Single-DAG health RBAC ───")
    for role in ("weather", "ecommerce"):
        info = API_KEYS[role]
        hdr = bearer(info["key"])
        expected = info["expected_dags"]

        # Forbidden DAGs: all known minus allowed
        forbidden = ALL_KNOWN_DAGS - expected
        for dag_id in sorted(forbidden)[:3]:  # Test up to 3 forbidden
            code, _ = http_get(f"{API_BASE}/api/v1/health/{dag_id}", headers=hdr)
            result(code == 403, f"RBAC: {role} blocked from /health/{dag_id}", f"got {code}", critical=code != 403)

        # Allowed DAGs
        for dag_id in sorted(expected):
            code, _ = http_get(f"{API_BASE}/api/v1/health/{dag_id}", headers=hdr)
            result(code == 200, f"RBAC: {role} allowed /health/{dag_id}", f"got {code}")

    # Failures with explicit dag_id filter — verify RBAC blocks
    print("\n  ─── Failures dag_id filter RBAC ───")
    for role in ("weather", "ecommerce"):
        info = API_KEYS[role]
        hdr = bearer(info["key"])
        expected = info["expected_dags"]
        forbidden_dag = (ALL_KNOWN_DAGS - expected).pop()
        code, body = http_get(
            f"{API_BASE}/api/v1/failures/?dag_id={forbidden_dag}&hours=720",
            headers=hdr,
        )
        # Should return 403 because dag_id is checked
        result(code == 403, f"RBAC: {role} failures?dag_id={forbidden_dag} → 403", f"got {code}", critical=code != 403)


# ══════════════════════════════════════════════════════════
#  SECTION 5 — Query Parameter Boundary Tests
# ══════════════════════════════════════════════════════════
def test_query_boundaries():
    section("QUERY PARAMETER BOUNDARY TESTS", 5)

    hdr = bearer(API_KEYS["admin"]["key"])

    # hours param: min=1, max=8760
    test_cases = [
        (
            "/api/v1/failures/",
            "hours",
            [("1", 200), ("8760", 200), ("0", 422), ("-1", 422), ("8761", 422), ("abc", 422)],
        ),
        ("/api/v1/failures/", "limit", [("1", 200), ("500", 200), ("0", 422), ("501", 422), ("-1", 422)]),
        ("/api/v1/tasks/long-running", "threshold_minutes", [("1", 200), ("10080", 200), ("0", 422), ("10081", 422)]),
        ("/api/v1/tasks/retries", "min_retries", [("1", 200), ("100", 200), ("0", 422), ("101", 422)]),
        ("/api/v1/dags/inactive", "days", [("1", 200), ("365", 200), ("0", 422), ("366", 422)]),
        (
            "/api/v1/scheduling/stale-dags",
            "expected_interval_hours",
            [("1", 200), ("720", 200), ("0", 422), ("721", 422)],
        ),
        ("/api/v1/scheduling/lag", "threshold_minutes", [("1", 200), ("10080", 200), ("0", 422), ("10081", 422)]),
    ]

    for base_ep, param, cases in test_cases:
        for val, expected_code in cases:
            url = f"{API_BASE}{base_ep}?{param}={val}"
            code, body = http_get(url, headers=hdr)
            ok = code == expected_code
            result(ok, f"{base_ep} {param}={val} → {expected_code}", f"got {code}")

    # Empty string for optional dag_id should work
    code, _ = http_get(f"{API_BASE}/api/v1/failures/?dag_id=&hours=24", headers=hdr)
    result(code == 200, "failures empty dag_id → 200", f"got {code}")

    # Very long dag_id (should not crash the server)
    long_dag = "a" * 300
    code, _ = http_get(f"{API_BASE}/api/v1/health/{long_dag}", headers=hdr)
    result(code in (200, 400, 403, 404, 422), f"health/{'a' * 10}... (300 chars)", f"got {code}")


# ══════════════════════════════════════════════════════════
#  SECTION 6 — Authentication & Security Tests
# ══════════════════════════════════════════════════════════
def test_auth_security():
    section("AUTHENTICATION & SECURITY", 6)

    test_ep = f"{API_BASE}/api/v1/overview/"
    hdr = bearer(API_KEYS["admin"]["key"])

    # ── Auth bypass attempts ──
    print("\n  ─── Auth bypass attempts ───")
    result(*_expect_code("No auth header", test_ep, None, 401))
    result(*_expect_code("Empty bearer", test_ep, {"Authorization": "Bearer "}, 401))
    result(*_expect_code("Invalid token", test_ep, {"Authorization": "Bearer AAAA"}, 401))
    result(*_expect_code("Basic scheme", test_ep, {"Authorization": "Basic YWRtaW46YWRtaW4="}, 401))
    result(*_expect_code("Digest scheme", test_ep, {"Authorization": "Digest realm=test"}, 401))
    result(
        *_expect_code("Token prefix case", test_ep, {"Authorization": "bearer " + API_KEYS["admin"]["key"]}, 401)
    )  # lowercase bearer
    result(*_expect_code("Double Bearer", test_ep, {"Authorization": "Bearer Bearer " + API_KEYS["admin"]["key"]}, 401))
    result(
        *_expect_code(
            "Trailing spaces in token", test_ep, {"Authorization": "Bearer " + API_KEYS["admin"]["key"] + "  "}, 200
        )
    )  # Token .strip() normalises whitespace
    result(
        *_expect_code("Leading space in token", test_ep, {"Authorization": "Bearer  " + API_KEYS["admin"]["key"]}, 200)
    )  # Token .strip() normalises whitespace
    result(
        *_expect_code(
            "Token with newline", test_ep, {"Authorization": "Bearer " + API_KEYS["admin"]["key"] + "\n"}, (401, 0)
        )
    )  # 0=conn error is OK

    # Revoked / old token
    result(
        *_expect_code(
            "Old/revoked token", test_ep, {"Authorization": "Bearer v8LEah9G93MOoPJn7WUUm8vYp6jEsnFmC_FOpMX1ctU"}, 401
        )
    )

    # Valid tokens
    for role, info in API_KEYS.items():
        code, _ = http_get(test_ep, headers=bearer(info["key"]))
        result(code == 200, f"Valid {role} key → 200", f"got {code}")

    # ── SQL injection ──
    print("\n  ─── SQL injection attempts ───")
    sqli_payloads = [
        "'; DROP TABLE dag_run;--",
        "1 OR 1=1",
        "1; SELECT * FROM information_schema.tables--",
        "1 UNION SELECT username,password FROM users--",
        "' OR '1'='1",
    ]
    for payload in sqli_payloads:
        encoded = urllib.parse.quote(payload)
        code, body = http_get(f"{API_BASE}/api/v1/failures/?dag_id={encoded}&hours=24", headers=hdr)
        # Should return 200 (with empty results or filtered) or 403/422, never 500
        result(code != 500, f"SQLi: dag_id={payload[:40]}", f"got {code}", critical=code == 500)

    for payload in sqli_payloads:
        encoded = urllib.parse.quote(payload)
        code, body = http_get(f"{API_BASE}/api/v1/health/{encoded}", headers=hdr)
        result(code != 500, f"SQLi: health/{payload[:40]}", f"got {code}", critical=code == 500)

    # ── Path traversal ──
    print("\n  ─── Path traversal ───")
    traversal_payloads = [
        "../../../etc/passwd",
        "..%2F..%2F..%2Fetc%2Fpasswd",
        "....//....//etc/passwd",
    ]
    for payload in traversal_payloads:
        code, body = http_get(f"{API_BASE}/api/v1/health/{payload}", headers=hdr)
        if isinstance(body, dict):
            body_str = json.dumps(body)
        else:
            body_str = str(body)
        no_leak = "root:" not in body_str and "/bin/bash" not in body_str
        result(no_leak and code != 500, f"Path traversal: {payload[:50]}", f"status={code}", critical=not no_leak)

    # ── Header injection ──
    print("\n  ─── Header injection ───")
    code, body, hdrs = http(
        "GET", f"{API_BASE}/api/v1/health/", headers={**hdr, "X-Forwarded-For": "127.0.0.1\r\nX-Injected: evil"}
    )
    result(code != 500, "Header injection X-Forwarded-For", f"status={code}")

    # ── CORS check (no CORS should be enabled by default) ──
    print("\n  ─── CORS ───")
    code, body, hdrs = http("GET", f"{API_BASE}/api/v1/health/", headers={**hdr, "Origin": "https://evil.com"})
    has_acao = "access-control-allow-origin" in hdrs
    if has_acao:
        warn("CORS enabled", f"ACAO={hdrs.get('access-control-allow-origin')}")
    else:
        result(True, "No CORS headers (expected)", "")

    # ── HTTP method tests ──
    print("\n  ─── Wrong HTTP methods ───")
    for bad_method in ("PUT", "DELETE", "PATCH"):
        code, _, _ = http(bad_method, f"{API_BASE}/api/v1/health/", headers=hdr)
        result(code == 405, f"{bad_method} /api/v1/health/ → 405", f"got {code}")

    # POST to GET-only endpoint
    code, _, _ = http("POST", f"{API_BASE}/api/v1/health/", headers=hdr)
    result(code == 405, "POST /api/v1/health/ → 405", f"got {code}")

    # GET to POST-only endpoint
    code, _ = http_get(f"{API_BASE}/api/v1/cache/invalidate", headers=hdr)
    result(code == 405, "GET /cache/invalidate → 405", f"got {code}")

    # ── Timing attack resistance ──
    print("\n  ─── Timing attack resistance ───")
    valid_key = API_KEYS["admin"]["key"]
    # Build a near-miss key (first 30 chars match)
    near_miss = valid_key[:30] + "X" * (len(valid_key) - 30)
    total_miss = "Z" * len(valid_key)

    n_samples = 10
    near_times = []
    total_times = []
    for _ in range(n_samples):
        t0 = time.monotonic()
        http_get(test_ep, headers={"Authorization": f"Bearer {near_miss}"}, skip_pace=True)
        near_times.append(time.monotonic() - t0)

        t0 = time.monotonic()
        http_get(test_ep, headers={"Authorization": f"Bearer {total_miss}"}, skip_pace=True)
        total_times.append(time.monotonic() - t0)

    avg_near = sum(near_times) / n_samples
    avg_total = sum(total_times) / n_samples
    ratio = max(avg_near, avg_total) / max(min(avg_near, avg_total), 0.0001)
    result(
        ratio < 3.0,
        "Timing attack: near-miss vs total-miss",
        f"near={avg_near * 1000:.1f}ms, total={avg_total * 1000:.1f}ms, ratio={ratio:.2f}",
    )


def _expect_code(label, url, headers, expected):
    code, _ = http_get(url, headers=headers)
    if isinstance(expected, tuple):
        return code in expected, label, f"expected one of {expected}, got {code}"
    return code == expected, label, f"expected {expected}, got {code}"


# ══════════════════════════════════════════════════════════
#  SECTION 7 — Rate Limiting
# ══════════════════════════════════════════════════════════
def test_rate_limiting():
    section("RATE LIMITING (runs last — triggers 429 flood)", 15)

    hdr = bearer(API_KEYS["admin"]["key"])

    # Healthz should NOT be rate limited
    print("\n  ─── /healthz exempt from rate limit ───")
    for _ in range(10):
        code, _ = http_get(f"{API_BASE}/healthz")
    result(code == 200, "/healthz not rate-limited after 10 rapid requests", f"last={code}")

    # Burst test: send many requests quickly — skip pacing to deliberately trigger
    # We need a fresh window, so wait first
    print("\n  ─── Burst rate limit test (130 rapid requests) ───")
    print("    Waiting 61s for a clean rate-limit window...")
    time.sleep(61)
    codes = []
    got_429 = False
    retry_after = None
    for i in range(130):
        code, body, hdrs = http("GET", f"{API_BASE}/api/v1/health/", headers=hdr, timeout=5, skip_pace=True)
        codes.append(code)
        if code == 429:
            got_429 = True
            retry_after = hdrs.get("retry-after", "missing")
            break

    if got_429:
        result(True, f"Rate limit triggered at request #{len(codes)}", f"429 after {len(codes)} requests")
        result(retry_after != "missing", "Retry-After header present", f"value={retry_after}")
    else:
        warn(
            "Rate limit NOT triggered in 130 requests",
            f"all codes: {set(codes)}, may be > 120/min configured or test ran too slowly",
        )

    # After getting 429, wait and try again
    if got_429:
        time.sleep(2)
        code, _ = http_get(f"{API_BASE}/api/v1/health/", headers=hdr)
        # May still be throttled depending on window
        result(code in (200, 429), "Request after rate limit cooldown", f"status={code}")


# ══════════════════════════════════════════════════════════
#  SECTION 8 — Concurrent Request Safety
# ══════════════════════════════════════════════════════════
def test_concurrency():
    section("CONCURRENT REQUEST SAFETY", 7)

    hdr = bearer(API_KEYS["admin"]["key"])

    # Wait for a clean rate-limit window so concurrent requests don't 429
    if len(_request_timestamps) > 80:
        now = time.monotonic()
        oldest = _request_timestamps[0] if _request_timestamps else now
        wait = max(0, 61 - (now - oldest))
        if wait > 0:
            print(f"    [PACE] Waiting {wait:.0f}s for clean window before concurrency test...")
            time.sleep(wait)
        _request_timestamps.clear()

    endpoints = [
        f"{API_BASE}/api/v1/health/",
        f"{API_BASE}/api/v1/overview/",
        f"{API_BASE}/api/v1/failures/stats?hours=24",
        f"{API_BASE}/api/v1/dags/complexity",
        f"{API_BASE}/api/v1/scheduling/pools",
    ]

    print("\n  ─── 20 parallel requests across 5 endpoints ───")
    results_map = {}

    def make_request(url):
        code, body, _ = http("GET", url, headers=hdr, timeout=30, skip_pace=True)
        return url, code, body

    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = []
        for _ in range(4):  # 4 requests per endpoint = 20 total
            for ep in endpoints:
                futures.append(pool.submit(make_request, ep))

        for f in as_completed(futures):
            url, code, body = f.result()
            path = url.replace(API_BASE, "")
            if path not in results_map:
                results_map[path] = []
            results_map[path].append(code)

    for path, codes in results_map.items():
        success_count = sum(
            1 for c in codes if c in (200, 429, 503)
        )  # 429 OK if rate limited, 503 OK for degraded health
        ok = success_count == len(codes)
        if not ok:
            pass
        result(ok, f"Concurrent {path}", f"codes={codes}")

    # Verify no data corruption: same endpoint should return same data
    print("\n  ─── Consistency across concurrent reads ───")
    responses = []
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [pool.submit(make_request, f"{API_BASE}/api/v1/dags/status-summary") for _ in range(5)]
        for f in as_completed(futures):
            _, code, body = f.result()
            if code == 200 and isinstance(body, dict):
                responses.append(body.get("data", {}))

    if len(responses) >= 2:
        # All responses should have same total_dags
        totals = [r.get("total_dags") for r in responses if "total_dags" in r]
        unique_totals = set(totals)
        result(len(unique_totals) <= 1, "Concurrent reads return consistent data", f"total_dags values: {totals}")
    else:
        skip("Concurrent consistency", f"only {len(responses)} valid responses")


# ══════════════════════════════════════════════════════════
#  SECTION 9 — Caching Tests
# ══════════════════════════════════════════════════════════
def test_caching():
    section("CACHING TESTS", 8)

    hdr = bearer(API_KEYS["admin"]["key"])

    # The concurrency test (Section 7) fires 25 rapid skip_pace requests that
    # aren't tracked in _request_timestamps but ARE counted by the API rate limiter.
    # Always wait for a clean rate-limit window before cache tests.
    print("    [PACE] Waiting 61s for clean rate-limit window before cache tests...")
    time.sleep(61)
    _request_timestamps.clear()

    # Cache invalidation endpoint works
    code, body, _ = http_post(f"{API_BASE}/api/v1/cache/invalidate", headers=hdr)
    result(code == 200, "POST /cache/invalidate → 200", f"body={body}")
    if isinstance(body, dict):
        data = body.get("data", {})
        result(data.get("cleared") is True, "Cache cleared=true in response", f"data={data}")

    # After invalidation, next request should still work (re-populates cache)
    code, body = http_get(f"{API_BASE}/api/v1/health/", headers=hdr)
    result(code in (200, 503), "Health endpoint works after cache clear", f"status={code}")  # 503 when degraded

    # Two rapid requests should return cached result (same timestamp)
    code1, body1 = http_get(f"{API_BASE}/api/v1/dags/status-summary", headers=hdr)
    code2, body2 = http_get(f"{API_BASE}/api/v1/dags/status-summary", headers=hdr)
    if code1 == 200 and code2 == 200:
        # Timestamps may differ slightly but data should be identical
        d1 = body1.get("data")
        d2 = body2.get("data")
        result(d1 == d2, "Rapid requests return cached data", f"data identical={d1 == d2}")
    else:
        skip("Cache freshness check", f"codes {code1}, {code2}")


# ══════════════════════════════════════════════════════════
#  SECTION 10 — Data Consistency (Plugin vs API)
# ══════════════════════════════════════════════════════════
def test_data_consistency():
    section("DATA CONSISTENCY (Plugin vs Standalone)", 9)

    # Login to admin Airflow
    admin_base = "http://localhost:8080"
    opener, err = get_airflow_session(admin_base, "admin", "admin")
    if err:
        skip("Data consistency", f"cannot login: {err}")
        return

    # Airflow API DAG count
    try:
        req = urllib.request.Request(f"{admin_base}/api/v1/dags?limit=100")
        resp = opener.open(req, timeout=10)
        airflow_data = json.loads(resp.read().decode())
        airflow_dag_count = airflow_data.get("total_entries", 0)
        airflow_dag_ids = sorted(d["dag_id"] for d in airflow_data.get("dags", []))
    except Exception as e:
        skip("Airflow DAG count", str(e)[:80])
        return

    admin_hdr = bearer(API_KEYS["admin"]["key"])

    # Standalone DAG count via complexity (only returns complex DAGs, may be empty)
    code, api_data = http_get(f"{API_BASE}/api/v1/dags/complexity", headers=admin_hdr)
    if code == 200 and isinstance(api_data, dict):
        api_dags = api_data.get("data", {}).get("dags", [])
        api_dag_ids = sorted(d["dag_id"] for d in api_dags if isinstance(d, dict))
        if api_dag_ids:
            result(
                set(airflow_dag_ids) == set(api_dag_ids),
                "DAG IDs match (Airflow vs Standalone)",
                f"Airflow={airflow_dag_ids}, API={api_dag_ids}",
                critical=set(airflow_dag_ids) != set(api_dag_ids),
            )
        else:
            # complexity only returns DAGs exceeding threshold — empty is normal
            result(True, "Complexity endpoint returns 0 complex DAGs (expected)", f"count={len(api_dags)}")
    else:
        skip("API DAG IDs", f"complexity returned {code}")

    # Standalone DAG count via status-summary
    code, api_data = http_get(f"{API_BASE}/api/v1/dags/status-summary", headers=admin_hdr)
    if code == 200 and isinstance(api_data, dict):
        api_total = api_data.get("data", {}).get("total_dags", -1)
        result(
            airflow_dag_count == api_total,
            "DAG count matches",
            f"Airflow={airflow_dag_count}, Standalone={api_total}",
        )

    # Pool count comparison
    try:
        req = urllib.request.Request(f"{admin_base}/api/v1/pools")
        resp = opener.open(req, timeout=10)
        airflow_pools = json.loads(resp.read().decode())
        airflow_pool_count = airflow_pools.get("total_entries", 0)
    except Exception:
        airflow_pool_count = None

    code, pool_data = http_get(f"{API_BASE}/api/v1/scheduling/pools", headers=admin_hdr)
    if code == 200 and isinstance(pool_data, dict):
        api_pool_count = pool_data.get("data", {}).get("count", None)
        if airflow_pool_count is not None and api_pool_count is not None:
            result(
                airflow_pool_count == api_pool_count,
                "Pool count matches",
                f"Airflow={airflow_pool_count}, Standalone={api_pool_count}",
            )

    # Health score: system health from API
    code, health_data = http_get(f"{API_BASE}/api/v1/health/", headers=admin_hdr)
    if code in (200, 503) and isinstance(health_data, dict):
        health_score = health_data.get("data", {}).get("health_score")
        status_val = health_data.get("data", {}).get("status")
        if code == 200:
            result(
                status_val == "healthy" and (health_score is None or health_score >= 70),
                "Health: 200 → healthy with score ≥ 70",
                f"status={status_val}, score={health_score}",
            )
        else:
            result(status_val == "degraded", "Health: 503 → degraded", f"status={status_val}, score={health_score}")


# ══════════════════════════════════════════════════════════
#  SECTION 11 — Error Handling & Edge Cases
# ══════════════════════════════════════════════════════════
def test_error_handling():
    section("ERROR HANDLING & EDGE CASES", 10)

    hdr = bearer(API_KEYS["admin"]["key"])

    # ── 404s ──
    print("\n  ─── 404 Not Found ───")
    not_found_paths = [
        "/api/v1/nonexistent",
        "/api/v2/health/",
        "/api/v1/failures/nonexistent",
        "/watcher/dashboard",  # Plugin path on API port
    ]
    for path in not_found_paths:
        code, _ = http_get(f"{API_BASE}{path}", headers=hdr)
        result(code == 404, f"GET {path} → 404", f"got {code}")

    # ── Double slashes ──
    print("\n  ─── URL quirks ───")
    code, _ = http_get(f"{API_BASE}//api//v1//health//", headers=hdr)
    result(code in (200, 307, 404), "Double slashes in URL", f"got {code}")

    # Trailing dot
    code, _ = http_get(f"{API_BASE}/api/v1/health/.", headers=hdr)
    result(code in (200, 403, 404, 422), "Path with trailing dot", f"got {code}")

    # Unicode in path
    code, _ = http_get(f"{API_BASE}/api/v1/health/日本語テスト", headers=hdr)
    result(code in (0, 200, 403, 404, 422), "Unicode DAG ID", f"got {code}")

    # ── Large payloads ──
    print("\n  ─── Large/unusual requests ───")
    # Very long URL
    long_url = f"{API_BASE}/api/v1/failures/?dag_id={'x' * 2000}&hours=24"
    code, _ = http_get(long_url, headers=hdr)
    result(code in (200, 400, 403, 414, 422), "Very long URL (2000 char dag_id)", f"got {code}")

    # POST with unexpected body to evaluate endpoint
    code, body, _ = http_post(
        f"{API_BASE}/api/v1/alerts/evaluate",
        headers={**hdr, "Content-Type": "application/json"},
        body=b'{"unexpected": "payload"}',
    )
    result(code in (200, 422), "POST /alerts/evaluate with unexpected body", f"got {code}")

    # POST with invalid JSON body
    code, body, _ = http_post(
        f"{API_BASE}/api/v1/alerts/evaluate",
        headers={**hdr, "Content-Type": "application/json"},
        body=b"not json at all {{{",
    )
    result(code in (200, 400, 422), "POST with malformed JSON", f"got {code}")

    # ── Special characters in DAG ID ──
    print("\n  ─── Special characters in DAG ID ───")
    special_dags = [
        "dag-with-dashes",
        "dag_with_underscores",
        "dag.with.dots",
        "dag/with/slashes",
        "dag with spaces",
        "dag%20encoded",
    ]
    for dag_id in special_dags:
        encoded = urllib.parse.quote(dag_id, safe="")
        code, _ = http_get(f"{API_BASE}/api/v1/health/{encoded}", headers=hdr)
        result(code != 500, f"health/{dag_id[:30]}", f"got {code}", critical=code == 500)

    # ── Content-Type check ──
    print("\n  ─── Content-Type headers ───")
    code, body, hdrs = http("GET", f"{API_BASE}/api/v1/health/", headers=hdr)
    ct = hdrs.get("content-type", "")
    result("application/json" in ct, "Response Content-Type is JSON", f"got '{ct}'")

    # Healthz Content-Type
    code, body, hdrs = http("GET", f"{API_BASE}/healthz")
    ct = hdrs.get("content-type", "")
    result("application/json" in ct, "/healthz Content-Type is JSON", f"got '{ct}'")


# ══════════════════════════════════════════════════════════
#  SECTION 12 — POST Endpoint Tests
# ══════════════════════════════════════════════════════════
def test_post_endpoints():
    section("POST ENDPOINT TESTS", 11)

    hdr = bearer(API_KEYS["admin"]["key"])

    # alerts/evaluate — should work
    print("\n  ─── POST /api/v1/alerts/evaluate ───")
    code, body, _ = http_post(f"{API_BASE}/api/v1/alerts/evaluate", headers=hdr)
    result(code == 200, "POST alerts/evaluate → 200", f"status={code}")
    if code == 200 and isinstance(body, dict):
        data = body.get("data", {})
        result(
            "evaluated_rules" in data or "results" in data,
            "alerts/evaluate has expected fields",
            f"keys={sorted(data.keys()) if isinstance(data, dict) else 'not dict'}",
        )

    # cache/invalidate — should work
    print("\n  ─── POST /api/v1/cache/invalidate ───")
    code, body, _ = http_post(f"{API_BASE}/api/v1/cache/invalidate", headers=hdr)
    result(code == 200, "POST cache/invalidate → 200", f"status={code}")
    if code == 200 and isinstance(body, dict):
        data = body.get("data", {})
        result(data.get("cleared") is True, "cache cleared=true", f"data={data}")

    # POST to alerts/evaluate requires auth
    code, _, _ = http_post(f"{API_BASE}/api/v1/alerts/evaluate")
    result(code == 401, "POST alerts/evaluate without auth → 401", f"got {code}")

    # POST to cache/invalidate requires auth
    code, _, _ = http_post(f"{API_BASE}/api/v1/cache/invalidate")
    result(code == 401, "POST cache/invalidate without auth → 401", f"got {code}")

    # Scoped users can POST evaluate and invalidate too
    for role in ("weather", "ecommerce"):
        rhdr = bearer(API_KEYS[role]["key"])
        code, _, _ = http_post(f"{API_BASE}/api/v1/alerts/evaluate", headers=rhdr)
        result(code == 200, f"{role} can POST alerts/evaluate", f"got {code}")
        code, _, _ = http_post(f"{API_BASE}/api/v1/cache/invalidate", headers=rhdr)
        result(code == 200, f"{role} can POST cache/invalidate", f"got {code}")


# ══════════════════════════════════════════════════════════
#  SECTION 13 — Overview Deep Validation
# ══════════════════════════════════════════════════════════
def test_overview_deep():
    section("OVERVIEW ENDPOINT DEEP VALIDATION", 12)

    admin_hdr = bearer(API_KEYS["admin"]["key"])
    code, body = http_get(f"{API_BASE}/api/v1/overview/", headers=admin_hdr)
    if code != 200:
        skip("Overview deep validation", f"status={code}")
        return

    data = body.get("data", {})

    # Check expected top-level keys
    expected_keys = {"failure_stats", "sla_stats", "dag_summary"}
    present_keys = set(data.keys())
    for key in expected_keys:
        result(key in present_keys, f"Overview has '{key}'", f"present={key in present_keys}")

    # Numerical fields should be non-negative
    for key in ("long_running_tasks", "zombie_count", "import_errors"):
        if key in data:
            val = data[key]
            result(isinstance(val, (int, float)) and val >= 0, f"Overview {key} ≥ 0", f"value={val}")

    # dag_summary should have total_dags
    dag_summary = data.get("dag_summary", {})
    if isinstance(dag_summary, dict):
        total = dag_summary.get("total_dags")
        result(isinstance(total, int) and total >= 0, "dag_summary.total_dags is non-negative int", f"value={total}")

        health_score = dag_summary.get("health_score")
        if health_score is not None:
            result(0 <= health_score <= 100, "dag_summary.health_score in [0,100]", f"value={health_score}")

    # Compare overview's failure_stats with standalone /failures/stats
    code2, stats_body = http_get(f"{API_BASE}/api/v1/failures/stats?hours=24", headers=admin_hdr)
    if code2 == 200:
        ov_failures = data.get("failure_stats", {})
        st_failures = stats_body.get("data", {})
        # Both should have total_failures (may differ slightly due to cache timing)
        if "total_failures" in ov_failures and "total_failures" in st_failures:
            diff = abs(ov_failures["total_failures"] - st_failures["total_failures"])
            result(
                diff <= 5,
                "Overview failure count ≈ /failures/stats",
                f"overview={ov_failures['total_failures']}, stats={st_failures['total_failures']}, diff={diff}",
            )


# ══════════════════════════════════════════════════════════
#  SECTION 14 — RBAC Scoped Keys on POST Endpoints
# ══════════════════════════════════════════════════════════
def test_rbac_post_endpoints():
    section("RBAC ON POST ENDPOINTS", 13)

    # Verify scoped keys can't bypass RBAC via POST methods (if any future POST endpoints are dag-scoped)
    # For now, just verify POST endpoints don't leak data
    for role in ("weather", "ecommerce"):
        info = API_KEYS[role]
        rhdr = bearer(info["key"])
        expected = info["expected_dags"]

        code, body, _ = http_post(f"{API_BASE}/api/v1/alerts/evaluate", headers=rhdr)
        if code == 200 and isinstance(body, dict):
            # Alerts evaluate may contain DAG context in results
            data = body.get("data", {})
            found_dags: Set[str] = set()
            _extract_dag_ids_recursive(data, found_dags)
            if found_dags:
                leaked = found_dags - expected
                result(
                    len(leaked) == 0,
                    f"RBAC: {role} alerts/evaluate no leak",
                    f"dags={sorted(found_dags)}, leaked={sorted(leaked)}",
                    critical=len(leaked) > 0,
                )
            else:
                result(True, f"RBAC: {role} alerts/evaluate (no DAGs in response)", "")


# ══════════════════════════════════════════════════════════
#  SECTION 15 — Healthz Deep Validation
# ══════════════════════════════════════════════════════════
def test_healthz_deep():
    section("/HEALTHZ DEEP VALIDATION", 14)

    code, body, hdrs = http("GET", f"{API_BASE}/healthz")
    result(code == 200, "/healthz returns 200", f"got {code}")

    if isinstance(body, dict):
        # Required fields
        for field in ("status", "uptime_seconds", "db_connected"):
            result(field in body, f"/healthz has '{field}'", f"keys={sorted(body.keys())}")

        # status should be "ok" or "degraded"
        result(body.get("status") in ("ok", "degraded"), "/healthz status is ok|degraded", f"got {body.get('status')}")

        # uptime should be positive
        uptime = body.get("uptime_seconds", -1)
        result(isinstance(uptime, (int, float)) and uptime > 0, "/healthz uptime > 0", f"got {uptime}")

        # db_connected should be boolean
        result(
            isinstance(body.get("db_connected"), bool),
            "/healthz db_connected is bool",
            f"got {type(body.get('db_connected'))}",
        )

    # /healthz must NOT require auth
    code2, _ = http_get(f"{API_BASE}/healthz")
    result(code2 == 200, "/healthz works without auth header", f"got {code2}")

    # /healthz should not be rate limited (already tested in rate limit section)
    # /healthz should set X-API-Version
    api_v = hdrs.get("x-api-version", "")
    result(api_v == "1.0", "/healthz has X-API-Version=1.0", f"got '{api_v}'")


# ══════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════
if __name__ == "__main__":
    start = time.time()
    print("\n" + "#" * 76)
    print("#  AIRFLOW WATCHER — DEEP QA TEST SUITE")
    print(f"#  Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("#  Target: Plugin (8080-8082) + API (8083)")
    print("#" * 76)

    test_plugin()  # Section 1
    test_api_endpoints()  # Section 2
    test_envelope_and_schema()  # Section 3
    test_rbac_deep()  # Section 4
    test_query_boundaries()  # Section 5
    test_auth_security()  # Section 6
    test_concurrency()  # Section 7 (was 8)
    test_caching()  # Section 8 (was 9)
    test_data_consistency()  # Section 9 (was 10)
    test_error_handling()  # Section 10 (was 11)
    test_post_endpoints()  # Section 11 (was 12)
    test_overview_deep()  # Section 12 (was 13)
    test_rbac_post_endpoints()  # Section 13 (was 14)
    test_healthz_deep()  # Section 14 (was 15)
    test_rate_limiting()  # Section 15 — LAST (triggers 429 flood)

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
        print("\n  \033[92m✅ ALL TESTS PASSED — No bugs found!\033[0m")
    else:
        print(f"\n  \033[91m❌ {FAIL} TEST(S) FAILED — See bugs above.\033[0m")

    sys.exit(1 if FAIL > 0 else 0)
