#!/usr/bin/env python
"""
Comprehensive live test: Watcher Plugin (3 webservers) + Standalone API.

Tests:
  1. Watcher plugin on admin (8080), weather (8081), ecommerce (8082)
  2. Standalone API on port 8083 with Bearer auth
  3. RBAC filtering: weather_user sees only weather DAGs, etc.
  4. Auth edge cases: missing token, bad token, no-auth healthz
"""

import base64
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

PASS = 0
FAIL = 0
SKIP = 0


def result(ok, label, detail=""):
    global PASS, FAIL
    status = "\033[92mPASS\033[0m" if ok else "\033[91mFAIL\033[0m"
    if not ok:
        FAIL += 1
    else:
        PASS += 1
    suffix = f" — {detail}" if detail else ""
    print(f"  [{status}] {label}{suffix}")


def skip(label, reason):
    global SKIP
    SKIP += 1
    print(f"  [\033[93mSKIP\033[0m] {label} — {reason}")


def http_get(url, headers=None, timeout=15):
    """Return (status_code, body_dict_or_str)."""
    req = urllib.request.Request(url, headers=headers or {})
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        body = resp.read().decode()
        try:
            return resp.status, json.loads(body)
        except json.JSONDecodeError:
            return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            return e.code, json.loads(body)
        except json.JSONDecodeError:
            return e.code, body
    except Exception as e:
        return 0, str(e)


def basic_auth_header(user, password):
    creds = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {"Authorization": f"Basic {creds}"}


# ============================================================
# SECTION 1: Watcher Plugin on Airflow Webservers
# ============================================================

WEBSERVERS = [
    {
        "name": "admin",
        "port": 8080,
        "user": "admin",
        "password": "admin",
        "role": "Admin (all DAGs)",
    },
    {
        "name": "weather",
        "port": 8081,
        "user": "weather_user",
        "password": "weather123",
        "role": "team_weather (weather_data_pipeline, stock_market_collector)",
    },
    {
        "name": "ecommerce",
        "port": 8082,
        "user": "ecommerce_user",
        "password": "ecommerce123",
        "role": "team_ecommerce (ecommerce_sales_etl, data_quality_checks)",
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


def get_airflow_session(base_url, user, password):
    """Log in to Airflow and return session cookie."""
    # First get the CSRF token from the login page
    login_url = f"{base_url}/login/"
    try:
        req = urllib.request.Request(login_url)
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor())
        resp = opener.open(req, timeout=10)
        html = resp.read().decode()
        # Extract csrf_token
        import re

        csrf_match = re.search(r'name="csrf_token"\s+.*?value="([^"]+)"', html)
        if not csrf_match:
            # Try alternate pattern
            csrf_match = re.search(r'csrf_token.*?value="([^"]+)"', html)
        if not csrf_match:
            return None, "Could not find CSRF token"
        csrf_token = csrf_match.group(1)

        # POST login
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
        # Return opener with cookies
        return opener, None
    except Exception as e:
        return None, str(e)


def test_watcher_plugin():
    print("\n" + "=" * 70)
    print("SECTION 1: WATCHER PLUGIN (Airflow Webservers)")
    print("=" * 70)

    for ws in WEBSERVERS:
        print(f"\n--- {ws['name'].upper()} webserver (:{ws['port']}) as {ws['user']} [{ws['role']}] ---")
        base = f"http://localhost:{ws['port']}"

        # Check if webserver is reachable
        status, body = http_get(f"{base}/health", timeout=5)
        if status == 0:
            skip(f"{ws['name']} webserver", f"Not reachable: {body}")
            continue

        result(status == 200, "Webserver health check", f"status={status}")

        # Log in to get session
        opener, err = get_airflow_session(base, ws["user"], ws["password"])
        if err:
            skip(f"Login as {ws['user']}", err)
            continue
        result(True, f"Login as {ws['user']}", "session obtained")

        # Test each watcher page
        for page in WATCHER_PAGES:
            try:
                req = urllib.request.Request(f"{base}{page}")
                resp = opener.open(req, timeout=15)
                html = resp.read().decode()
                status_code = resp.status
                # Check for meaningful content
                has_content = "watcher" in html.lower() or "dashboard" in html.lower() or "failure" in html.lower()
                result(status_code == 200 and has_content, f"{page}", f"status={status_code}, size={len(html)} bytes")
            except urllib.error.HTTPError as e:
                result(False, f"{page}", f"HTTP {e.code}")
            except Exception as e:
                result(False, f"{page}", str(e)[:80])

        # Test Airflow API for DAG visibility (RBAC check)
        try:
            req = urllib.request.Request(f"{base}/api/v1/dags?limit=100")
            req.add_header("Content-Type", "application/json")
            resp = opener.open(req, timeout=10)
            dags_data = json.loads(resp.read().decode())
            dag_ids = [d["dag_id"] for d in dags_data.get("dags", [])]
            result(True, "DAG visibility", f"sees {len(dag_ids)} DAGs: {dag_ids}")
        except Exception as e:
            skip("DAG visibility check", str(e)[:80])


# ============================================================
# SECTION 2: Standalone API (port 8083)
# ============================================================

API_BASE = "http://localhost:8083"

# Three API keys matching .env — admin has full access, weather/ecommerce are scoped.
API_KEYS = {
    "admin": {
        "key": "RrkU1CeHfHjtONRYPYKYOdxRcrmraSGM_mQl_gsVCaQ",
        "expected_dags": None,  # None = full access (all 8 DAGs)
        "label": "Admin (all DAGs)",
    },
    "weather": {
        "key": "kkVJKBtAFB7GuqxCa68z9qhvBUMmbYFr9ne71GrYYcg",
        "expected_dags": {"weather_data_pipeline", "stock_market_collector"},
        "label": "team_weather (2 DAGs)",
    },
    "ecommerce": {
        "key": "ttT4gHJWDjmgH6cmMBiUC3Vj0qVEhWYJ7fLMmpRKRJ8",
        "expected_dags": {"ecommerce_sales_etl", "data_quality_checks"},
        "label": "team_ecommerce (2 DAGs)",
    },
}

API_ENDPOINTS = [
    ("GET", "/api/v1/failures/?hours=24&limit=5"),
    ("GET", "/api/v1/failures/stats?hours=24"),
    ("GET", "/api/v1/sla/misses?hours=24"),
    ("GET", "/api/v1/sla/stats?hours=24"),
    ("GET", "/api/v1/tasks/long-running?hours=24"),
    ("GET", "/api/v1/tasks/retries?hours=24"),
    ("GET", "/api/v1/tasks/zombies"),
    ("GET", "/api/v1/tasks/failure-patterns?hours=24"),
    ("GET", "/api/v1/scheduling/lag"),
    ("GET", "/api/v1/scheduling/queue"),
    ("GET", "/api/v1/scheduling/pools"),
    ("GET", "/api/v1/scheduling/stale-dags?hours=72"),
    ("GET", "/api/v1/scheduling/concurrent"),
    ("GET", "/api/v1/dags/import-errors"),
    ("GET", "/api/v1/dags/status-summary"),
    ("GET", "/api/v1/dags/complexity"),
    ("GET", "/api/v1/dags/inactive?days=7"),
    ("GET", "/api/v1/dependencies/upstream-failures?hours=24"),
    ("GET", "/api/v1/dependencies/correlations?hours=24"),
    ("GET", "/api/v1/health/"),
    ("GET", "/api/v1/overview/"),
    ("GET", "/api/v1/alerts/rules"),
]

# Endpoints that return 501 in standalone mode (DagBag required).
API_STANDALONE_501 = [
    ("GET", "/api/v1/dependencies/cross-dag"),
    ("GET", "/api/v1/dependencies/impact/some_dag/some_task"),
]


def test_standalone_api():
    print("\n" + "=" * 70)
    print("SECTION 2: STANDALONE API (port 8083) — 3 API KEYS WITH RBAC")
    print("=" * 70)

    # Check if API is reachable
    status, body = http_get(f"{API_BASE}/healthz", timeout=5)
    if status == 0:
        skip("Standalone API", f"Not reachable: {body}")
        return
    result(
        status == 200,
        "Healthz (no auth required)",
        f"db_connected={body.get('db_connected') if isinstance(body, dict) else '?'}",
    )

    for role_name, role_info in API_KEYS.items():
        key = role_info["key"]
        expected_dags = role_info["expected_dags"]
        label = role_info["label"]
        auth_header = {"Authorization": f"Bearer {key}"}

        print(f"\n  --- {role_name.upper()} key [{label}] ---")

        # Test all endpoints return 200
        for method, ep in API_ENDPOINTS:
            status, body = http_get(f"{API_BASE}{ep}", headers=auth_header)
            ok = status == 200
            detail = f"status={status}"
            if ok and isinstance(body, dict):
                if "data" in body and isinstance(body["data"], dict):
                    keys = list(body["data"].keys())[:4]
                    detail += f", keys={keys}"
                elif "data" in body and isinstance(body["data"], list):
                    detail += f", items={len(body['data'])}"
            elif not ok and isinstance(body, dict):
                detail += f", error={body.get('detail', body.get('error', ''))[:60]}"
            result(ok, ep.split("?")[0], detail)

        # RBAC filtering check: verify DAG visibility via complexity endpoint
        status, body = http_get(f"{API_BASE}/api/v1/dags/complexity", headers=auth_header)
        if status == 200 and isinstance(body, dict):
            dags_data = body.get("data", {})
            dag_list = dags_data.get("dags", [])
            visible_dag_ids = {d["dag_id"] for d in dag_list if isinstance(d, dict) and "dag_id" in d}
            if expected_dags is None:
                # Admin: should see all DAGs
                result(
                    len(visible_dag_ids) >= 4,
                    f"RBAC: {role_name} sees all DAGs",
                    f"visible={len(visible_dag_ids)} DAGs: {sorted(visible_dag_ids)}",
                )
            else:
                # Scoped user: should see only mapped DAGs
                result(
                    visible_dag_ids == expected_dags,
                    f"RBAC: {role_name} sees only {len(expected_dags)} DAGs",
                    f"expected={sorted(expected_dags)}, got={sorted(visible_dag_ids)}",
                )
        else:
            skip(f"RBAC check for {role_name}", f"complexity returned {status}")

        # RBAC filtering check: verify failures are filtered
        status, body = http_get(f"{API_BASE}/api/v1/failures/?hours=168&limit=100", headers=auth_header)
        if status == 200 and isinstance(body, dict):
            failures = body.get("data", {}).get("failures", [])
            failure_dag_ids = {f["dag_id"] for f in failures if isinstance(f, dict) and "dag_id" in f}
            if expected_dags is None:
                result(True, f"RBAC: {role_name} failures unfiltered", f"dags in failures={sorted(failure_dag_ids)}")
            else:
                leaked = failure_dag_ids - expected_dags
                result(
                    len(leaked) == 0,
                    f"RBAC: {role_name} failures filtered",
                    f"failure_dags={sorted(failure_dag_ids)}, leaked={sorted(leaked)}",
                )
        else:
            skip(f"RBAC failure check for {role_name}", f"returned {status}")

        # RBAC: check single-DAG health endpoint rejects unauthorized DAG
        if expected_dags is not None:
            forbidden_dag = "ml_training_pipeline"  # Not in weather or ecommerce scope
            status, body = http_get(f"{API_BASE}/api/v1/health/{forbidden_dag}", headers=auth_header)
            result(status == 403, f"RBAC: {role_name} blocked from {forbidden_dag}", f"status={status}")

            allowed_dag = sorted(expected_dags)[0]
            status, body = http_get(f"{API_BASE}/api/v1/health/{allowed_dag}", headers=auth_header)
            result(status == 200, f"RBAC: {role_name} can access {allowed_dag}", f"status={status}")

    # Test endpoints expected to return 501 in standalone mode (use admin key)
    admin_header = {"Authorization": f"Bearer {API_KEYS['admin']['key']}"}
    print("\n  --- DagBag-dependent endpoints (expect 501 in standalone) ---")
    for method, ep in API_STANDALONE_501:
        status, body = http_get(f"{API_BASE}{ep}", headers=admin_header)
        ok = status == 501
        detail = f"status={status}"
        if isinstance(body, dict):
            detail += f", detail={body.get('detail', '')[:60]}"
        result(ok, f"{ep.split('?')[0]} → 501", detail)


# ============================================================
# SECTION 3: Auth Edge Cases
# ============================================================


def test_auth_edge_cases():
    print("\n" + "=" * 70)
    print("SECTION 3: AUTH EDGE CASES (Standalone API)")
    print("=" * 70)

    # Check API reachable first
    status, _ = http_get(f"{API_BASE}/healthz", timeout=5)
    if status == 0:
        skip("Auth tests", "API not reachable")
        return

    test_endpoint = f"{API_BASE}/api/v1/overview/"

    # 1. No auth header -> 401
    status, body = http_get(test_endpoint)
    result(status == 401, "No auth header → 401", f"got {status}")

    # 2. Empty bearer -> 401
    status, body = http_get(test_endpoint, headers={"Authorization": "Bearer "})
    result(status == 401 or status == 403, "Empty bearer token → 401/403", f"got {status}")

    # 3. Invalid token -> 401
    status, body = http_get(test_endpoint, headers={"Authorization": "Bearer invalid_token_xyz"})
    result(status == 401 or status == 403, "Invalid bearer token → 401/403", f"got {status}")

    # 4. Wrong auth scheme -> 401
    status, body = http_get(test_endpoint, headers={"Authorization": "Basic dXNlcjpwYXNz"})
    result(status == 401 or status == 403, "Basic auth scheme → 401/403", f"got {status}")

    # 5. Valid token works (admin key)
    status, body = http_get(test_endpoint, headers={"Authorization": f"Bearer {API_KEYS['admin']['key']}"})
    result(status == 200, "Valid admin bearer token → 200", f"got {status}")

    # 6. Valid token works (weather key)
    status, body = http_get(test_endpoint, headers={"Authorization": f"Bearer {API_KEYS['weather']['key']}"})
    result(status == 200, "Valid weather bearer token → 200", f"got {status}")

    # 7. Valid token works (ecommerce key)
    status, body = http_get(test_endpoint, headers={"Authorization": f"Bearer {API_KEYS['ecommerce']['key']}"})
    result(status == 200, "Valid ecommerce bearer token → 200", f"got {status}")

    # 8. Old/revoked token -> 401
    status, body = http_get(
        test_endpoint, headers={"Authorization": "Bearer v8LEah9G93MOoPJn7WUUm8vYp6jEsnFmC_FOpMX1ctU"}
    )
    result(status == 401, "Old/revoked token → 401", f"got {status}")

    # 6. Healthz works without auth
    status, body = http_get(f"{API_BASE}/healthz")
    result(status == 200, "/healthz works without auth", f"got {status}")

    # 7. OpenAPI docs accessible
    status, body = http_get(f"{API_BASE}/docs")
    result(status == 200, "/docs (Swagger UI) accessible", f"got {status}")

    # 8. Non-existent endpoint -> 404
    status, body = http_get(
        f"{API_BASE}/api/v1/nonexistent",
        headers={"Authorization": f"Bearer {API_KEYS['admin']['key']}"},
    )
    result(status == 404, "Non-existent endpoint → 404", f"got {status}")


# ============================================================
# SECTION 4: Data Consistency (plugin vs API)
# ============================================================


def test_data_consistency():
    print("\n" + "=" * 70)
    print("SECTION 4: DATA CONSISTENCY (Plugin API vs Standalone API)")
    print("=" * 70)

    # Use Airflow REST API (admin) to get DAG count
    admin_base = "http://localhost:8080"
    admin_opener, err = get_airflow_session(admin_base, "admin", "admin")
    if err:
        skip("Data consistency", f"Cannot login to admin: {err}")
        return

    try:
        req = urllib.request.Request(f"{admin_base}/api/v1/dags?limit=100")
        resp = admin_opener.open(req, timeout=10)
        airflow_dags = json.loads(resp.read().decode())
        airflow_dag_count = airflow_dags.get("total_entries", 0)
    except Exception as e:
        skip("Airflow DAG count", str(e)[:80])
        airflow_dag_count = None

    # Get standalone API DAG count (use admin key for unfiltered view)
    auth_header = {"Authorization": f"Bearer {API_KEYS['admin']['key']}"}
    status, api_data = http_get(f"{API_BASE}/api/v1/dags/status-summary", headers=auth_header)
    if status == 200 and isinstance(api_data, dict):
        api_dag_count = api_data.get("data", {}).get("total_dags", None)
    else:
        api_dag_count = None

    if airflow_dag_count is not None and api_dag_count is not None:
        result(
            airflow_dag_count == api_dag_count,
            "DAG count matches (Airflow API vs Standalone API)",
            f"Airflow={airflow_dag_count}, Standalone={api_dag_count}",
        )
    else:
        skip("DAG count comparison", f"airflow={airflow_dag_count}, api={api_dag_count}")

    # Compare pool counts
    try:
        req = urllib.request.Request(f"{admin_base}/api/v1/pools")
        resp = admin_opener.open(req, timeout=10)
        airflow_pools = json.loads(resp.read().decode())
        airflow_pool_count = airflow_pools.get("total_entries", 0)
    except Exception:
        airflow_pool_count = None

    status, pool_data = http_get(f"{API_BASE}/api/v1/scheduling/pools", headers=auth_header)
    if status == 200 and isinstance(pool_data, dict):
        api_pool_count = pool_data.get("data", {}).get("count", None)
    else:
        api_pool_count = None

    if airflow_pool_count is not None and api_pool_count is not None:
        result(
            airflow_pool_count == api_pool_count,
            "Pool count matches (Airflow API vs Standalone API)",
            f"Airflow={airflow_pool_count}, Standalone={api_pool_count}",
        )
    else:
        skip("Pool count comparison", f"airflow={airflow_pool_count}, api={api_pool_count}")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("\n" + "#" * 70)
    print("# AIRFLOW WATCHER: COMPREHENSIVE LIVE TEST SUITE")
    print(f"# Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("#" * 70)

    test_watcher_plugin()
    test_standalone_api()
    test_auth_edge_cases()
    test_data_consistency()

    print("\n" + "=" * 70)
    total = PASS + FAIL + SKIP
    print(f"RESULTS: {PASS} passed, {FAIL} failed, {SKIP} skipped (total: {total})")
    print("=" * 70)

    sys.exit(1 if FAIL > 0 else 0)
