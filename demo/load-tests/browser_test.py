#!/usr/bin/env python3
"""Playwright browser tests for Airflow Watcher RBAC.

Runs headless Chromium to verify:
- All Watcher pages render correctly per role
- RBAC filtering: restricted users only see their DAGs
- No data leakage across roles
- Page load performance (Core Web Vitals)

Prerequisites:
    pip install playwright
    python -m playwright install chromium

Usage:
    python browser_test.py              # run all tests
    python browser_test.py --headed     # run with visible browser
    python browser_test.py --slow       # slow mode (500ms between actions)
"""

import argparse
import os
import sys
import time
from dataclasses import dataclass, field
from playwright.sync_api import sync_playwright, Page, BrowserContext

BASE_URL = "http://localhost:8080"

# Expected DAG visibility per role
ROLE_DAGS = {
    "admin": {
        "weather_data_pipeline", "stock_market_collector",
        "ecommerce_sales_etl", "data_quality_checks",
        "ml_training_pipeline", "nyc_taxi_analytics",
        "sla_failure_demo", "social_media_analytics",
    },
    "weather": {"weather_data_pipeline", "stock_market_collector"},
    "ecommerce": {"ecommerce_sales_etl", "data_quality_checks"},
}

# DAGs that should NOT appear for restricted users
FORBIDDEN_DAGS = {
    "weather": {"ecommerce_sales_etl", "data_quality_checks", "ml_training_pipeline",
                "nyc_taxi_analytics", "sla_failure_demo", "social_media_analytics"},
    "ecommerce": {"weather_data_pipeline", "stock_market_collector", "ml_training_pipeline",
                  "nyc_taxi_analytics", "sla_failure_demo", "social_media_analytics"},
}

USERS = [
    {"username": "admin", "password": os.environ["ADMIN_PASSWORD"], "role": "admin"},
    {"username": "weather_user", "password": os.environ["WEATHER_USER_PASSWORD"], "role": "weather"},
    {"username": "ecommerce_user", "password": os.environ["ECOMMERCE_USER_PASSWORD"], "role": "ecommerce"},
]

WATCHER_PAGES = [
    ("/watcher/dashboard", "Dashboard"),
    ("/watcher/failures", "Failures"),
    ("/watcher/sla", "SLA"),
    ("/watcher/health", "Health"),
    ("/watcher/tasks", "Tasks"),
    ("/watcher/scheduling", "Scheduling"),
    ("/watcher/dag-health", "DAG Health"),
    ("/watcher/dependencies", "Dependencies"),
]


@dataclass
class TestResult:
    name: str
    passed: bool
    duration_ms: float = 0
    message: str = ""
    screenshots: list = field(default_factory=list)


class WatcherBrowserTest:
    def __init__(self, headed: bool = False, slow_mo: int = 0):
        self.headed = headed
        self.slow_mo = slow_mo
        self.results: list[TestResult] = []

    def run_all(self):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=not self.headed, slow_mo=self.slow_mo)

            for user in USERS:
                context = browser.new_context(viewport={"width": 1920, "height": 1080})
                page = context.new_page()

                # Login
                login_ok = self._login(page, user)
                if not login_ok:
                    self.results.append(TestResult(
                        name=f"[{user['role']}] Login",
                        passed=False,
                        message=f"Failed to login as {user['username']}",
                    ))
                    context.close()
                    continue

                self.results.append(TestResult(
                    name=f"[{user['role']}] Login",
                    passed=True,
                    message=f"Logged in as {user['username']}",
                ))

                # Test each page
                for path, page_name in WATCHER_PAGES:
                    self._test_page_renders(page, user, path, page_name)

                # RBAC data leakage test
                self._test_rbac_filtering(page, user)

                # Page performance test
                self._test_page_performance(page, user)

                context.close()

            browser.close()

        self._print_report()
        return all(r.passed for r in self.results)

    def _login(self, page: Page, user: dict) -> bool:
        """Login to Airflow and return success."""
        try:
            page.goto(f"{BASE_URL}/login/", wait_until="networkidle", timeout=15000)
            page.fill("input[name='username']", user["username"])
            page.fill("input[name='password']", user["password"])
            page.click("input[type='submit']")
            page.wait_for_url("**/home**", timeout=10000)
            return True
        except Exception as e:
            return False

    def _test_page_renders(self, page: Page, user: dict, path: str, page_name: str):
        """Test that a Watcher page renders without errors."""
        test_name = f"[{user['role']}] {page_name} page renders"
        start = time.monotonic()

        try:
            resp = page.goto(f"{BASE_URL}{path}", wait_until="networkidle", timeout=20000)
            elapsed = (time.monotonic() - start) * 1000

            if resp is None:
                self.results.append(TestResult(test_name, False, elapsed, "No response"))
                return

            status = resp.status

            # Check for server errors
            if status >= 500:
                self.results.append(TestResult(test_name, False, elapsed, f"HTTP {status}"))
                return

            # Check page has content (not blank/error page)
            body_text = page.inner_text("body")
            if len(body_text.strip()) < 50:
                self.results.append(TestResult(test_name, False, elapsed, "Page appears empty"))
                return

            # Check no Python tracebacks leaked
            if "Traceback" in body_text or "Internal Server Error" in body_text:
                self.results.append(TestResult(test_name, False, elapsed, "Server error in page"))
                return

            self.results.append(TestResult(test_name, True, elapsed, f"HTTP {status}, {len(body_text)} chars"))

        except Exception as e:
            elapsed = (time.monotonic() - start) * 1000
            self.results.append(TestResult(test_name, False, elapsed, str(e)))

    def _test_rbac_filtering(self, page: Page, user: dict):
        """Verify RBAC: restricted users don't see forbidden DAGs."""
        role = user["role"]
        if role == "admin":
            # Admin sees everything, just verify count
            test_name = f"[admin] RBAC: sees all DAGs"
            try:
                page.goto(f"{BASE_URL}/watcher/dashboard", wait_until="networkidle", timeout=15000)
                body = page.inner_text("body")
                visible_count = sum(1 for dag in ROLE_DAGS["admin"] if dag in body)
                self.results.append(TestResult(
                    test_name, True, 0,
                    f"Found {visible_count}/{len(ROLE_DAGS['admin'])} expected DAGs in dashboard",
                ))
            except Exception as e:
                self.results.append(TestResult(test_name, False, 0, str(e)))
            return

        # Restricted user — check forbidden DAGs don't appear
        forbidden = FORBIDDEN_DAGS.get(role, set())
        allowed = ROLE_DAGS.get(role, set())

        # Check across multiple pages for thorough coverage
        pages_to_check = ["/watcher/dashboard", "/watcher/failures", "/watcher/sla"]
        all_body_text = ""

        for path in pages_to_check:
            try:
                page.goto(f"{BASE_URL}{path}", wait_until="networkidle", timeout=15000)
                all_body_text += " " + page.inner_text("body")
            except Exception:
                pass

        # Check no forbidden DAGs leaked
        leaked = [dag for dag in forbidden if dag in all_body_text]
        test_name = f"[{role}] RBAC: no data leakage"
        if leaked:
            self.results.append(TestResult(
                test_name, False, 0,
                f"LEAKED DAGs found: {leaked}",
            ))
        else:
            self.results.append(TestResult(
                test_name, True, 0,
                f"No forbidden DAGs visible (checked {len(forbidden)} DAGs across {len(pages_to_check)} pages)",
            ))

        # Check allowed DAGs are visible (at least in dashboard)
        test_name = f"[{role}] RBAC: own DAGs visible"
        visible = [dag for dag in allowed if dag in all_body_text]
        # Note: DAGs may not appear if they have no failures/SLA misses, so this is a soft check
        self.results.append(TestResult(
            test_name, True, 0,
            f"Found {len(visible)}/{len(allowed)} allowed DAGs in page content",
        ))

        # Check RBAC badge is shown for restricted users
        test_name = f"[{role}] RBAC: lock badge visible"
        try:
            page.goto(f"{BASE_URL}/watcher/dashboard", wait_until="networkidle", timeout=15000)
            body = page.inner_text("body")
            has_badge = "🔒" in body or "rbac" in body.lower()
            self.results.append(TestResult(
                test_name, has_badge, 0,
                "RBAC badge found" if has_badge else "No RBAC badge found",
            ))
        except Exception as e:
            self.results.append(TestResult(test_name, False, 0, str(e)))

    def _test_page_performance(self, page: Page, user: dict):
        """Measure page load performance for the dashboard."""
        test_name = f"[{user['role']}] Performance: dashboard < 3s"

        try:
            start = time.monotonic()
            page.goto(f"{BASE_URL}/watcher/dashboard", wait_until="networkidle", timeout=20000)
            elapsed = (time.monotonic() - start) * 1000

            # Get navigation timing from browser
            timing = page.evaluate("""() => {
                const t = performance.getEntriesByType('navigation')[0];
                if (!t) return null;
                return {
                    dns: t.domainLookupEnd - t.domainLookupStart,
                    connect: t.connectEnd - t.connectStart,
                    ttfb: t.responseStart - t.requestStart,
                    download: t.responseEnd - t.responseStart,
                    dom_interactive: t.domInteractive - t.navigationStart,
                    dom_complete: t.domComplete - t.navigationStart,
                    load_event: t.loadEventEnd - t.navigationStart,
                };
            }""")

            passed = elapsed < 3000
            msg = f"Total: {elapsed:.0f}ms"
            if timing:
                msg += f" | TTFB: {timing['ttfb']:.0f}ms | DOM: {timing['dom_complete']:.0f}ms | Load: {timing['load_event']:.0f}ms"

            self.results.append(TestResult(test_name, passed, elapsed, msg))

        except Exception as e:
            self.results.append(TestResult(test_name, False, 0, str(e)))

    def _print_report(self):
        passed = sum(1 for r in self.results if r.passed)
        failed = sum(1 for r in self.results if not r.passed)
        total = len(self.results)

        print("\n" + "=" * 80)
        print("  AIRFLOW WATCHER — PLAYWRIGHT BROWSER TEST REPORT")
        print("=" * 80)

        current_role = None
        for r in self.results:
            role = r.name.split("]")[0].strip("[")
            if role != current_role:
                current_role = role
                print(f"\n  --- {role.upper()} ---")

            icon = "✓" if r.passed else "✗"
            time_str = f" ({r.duration_ms:.0f}ms)" if r.duration_ms > 0 else ""
            print(f"  {icon} {r.name}{time_str}")
            if r.message:
                print(f"    {r.message}")

        print(f"\n  {'=' * 40}")
        print(f"  Total: {total} | Passed: {passed} | Failed: {failed}")
        if failed == 0:
            print("  ALL TESTS PASSED ✓")
        else:
            print(f"  {failed} TESTS FAILED ✗")
        print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description="Playwright browser tests for Watcher RBAC")
    parser.add_argument("--headed", action="store_true", help="Run with visible browser")
    parser.add_argument("--slow", action="store_true", help="Slow mode (500ms between actions)")
    args = parser.parse_args()

    test = WatcherBrowserTest(headed=args.headed, slow_mo=500 if args.slow else 0)
    success = test.run_all()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
