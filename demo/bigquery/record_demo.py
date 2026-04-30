"""Automated demo recording for the Airflow Watcher BigQuery PoC.

Drives a real Chromium browser, records video, converts to MP4.

Usage:
    python record_demo.py

Output:
    demo_output/airflow_watcher_bq_demo.mp4
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE_URL = "http://localhost:8090"
OUT_DIR = Path(__file__).parent / "demo_output"
WEBM = OUT_DIR / "demo_raw.webm"
MP4 = OUT_DIR / "airflow_watcher_bq_demo.mp4"

# Timing knobs (seconds) ─────────────────────────────────────────────────────
PAUSE_LANDING = 3.5      # read landing page
PAUSE_CARD_SCROLL = 2.0  # cards visible
PAUSE_DOCS_LOAD = 2.5    # swagger UI settles
PAUSE_SECTION = 1.5      # between sections
PAUSE_RESPONSE = 4.0     # read API response
PAUSE_END = 2.0          # hold final frame


def wait(seconds: float) -> None:
    time.sleep(seconds)


# ─────────────────────────────────────────────────────────────────────────────
# Swagger helpers
# ─────────────────────────────────────────────────────────────────────────────

def scroll_to_section(page: Page, tag: str) -> None:
    """Scroll the tag header into view so the section is prominent on screen."""
    header = page.locator(f'[data-tag="{tag}"]').first
    header.scroll_into_view_if_needed()
    wait(0.6)


def demo_endpoint(page: Page, operation_id: str) -> None:
    """Click an operation to expand it, Try it out → Execute, show response."""
    op = page.locator(f"#{operation_id}").first

    # Scroll summary into view and click to expand
    op.locator(".opblock-summary").first.scroll_into_view_if_needed()
    wait(0.4)
    op.locator(".opblock-summary").first.click()
    wait(1.0)

    # Try it out
    try_btn = op.locator(".try-out__btn").first
    try_btn.scroll_into_view_if_needed()
    try_btn.click()
    wait(0.6)

    # Execute
    execute_btn = op.locator(".execute").first
    execute_btn.scroll_into_view_if_needed()
    execute_btn.click()
    wait(0.5)

    # Wait for the live response body (not the schema example)
    response_body = op.locator(".response-col_description pre.microlight:not(.example)")
    try:
        # Wait for loading spinner to disappear first (BQ queries can be slow)
        op.locator(".loading-container").wait_for(state="hidden", timeout=60_000)
    except Exception:
        pass
    try:
        response_body.first.wait_for(state="visible", timeout=60_000)
    except Exception:
        pass

    # Scroll response into view and hold for reading
    try:
        response_body.first.scroll_into_view_if_needed()
    except Exception:
        pass
    wait(PAUSE_RESPONSE)

    # Collapse the operation block
    op.locator(".opblock-summary").first.click()
    wait(0.6)


# ─────────────────────────────────────────────────────────────────────────────
# Main recording
# ─────────────────────────────────────────────────────────────────────────────

def record(page: Page) -> None:
    # ── 1. Landing page ──────────────────────────────────────────────────────
    page.goto(BASE_URL, wait_until="networkidle")
    wait(PAUSE_LANDING)

    # Scroll down to show cards
    page.evaluate("window.scrollTo({ top: 400, behavior: 'smooth' })")
    wait(PAUSE_CARD_SCROLL)

    # Back to top before CTA click
    page.evaluate("window.scrollTo({ top: 0, behavior: 'smooth' })")
    wait(0.8)

    # Click "Open Interactive API Docs"
    page.locator("a.cta").first.click()
    page.wait_for_url("**/docs", timeout=8_000)
    wait(PAUSE_DOCS_LOAD)

    # ── 2. Overview ───────────────────────────────────────────────────────────
    scroll_to_section(page, "overview")
    demo_endpoint(page, "operations-overview-get_overview_api_v1_overview_get")
    wait(PAUSE_SECTION)

    # ── 3. Failures ───────────────────────────────────────────────────────────
    scroll_to_section(page, "failures")
    demo_endpoint(page, "operations-failures-get_failure_stats_api_v1_failures_stats_get")
    wait(PAUSE_SECTION)

    # ── 4. dbt cost ───────────────────────────────────────────────────────────
    scroll_to_section(page, "dbt")
    demo_endpoint(page, "operations-dbt-get_cost_analysis_api_v1_dbt_cost_get")
    wait(PAUSE_SECTION)

    # ── 5. Task model performance ─────────────────────────────────────────────
    scroll_to_section(page, "tasks")
    demo_endpoint(page, "operations-tasks-get_model_performance_api_v1_tasks_model_performance_get")

    # Hold final frame at top
    page.evaluate("window.scrollTo({ top: 0, behavior: 'smooth' })")
    wait(PAUSE_END)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Recording demo…")
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,           # visible window looks better on screen recordings
            slow_mo=60,               # slight slowdown so actions are readable
            args=[
                "--window-size=1440,900",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            record_video_dir=str(OUT_DIR),
            record_video_size={"width": 1440, "height": 900},
        )
        page = context.new_page()

        try:
            record(page)
        finally:
            context.close()
            browser.close()

    # Playwright saves webm files with auto-generated names — find the latest
    webm_files = sorted(OUT_DIR.glob("*.webm"), key=lambda f: f.stat().st_mtime)
    if not webm_files:
        print("ERROR: no webm file found in demo_output/")
        sys.exit(1)
    latest_webm = webm_files[-1]
    latest_webm.rename(WEBM)
    print(f"Raw recording: {WEBM}")

    # Convert webm → mp4
    print("Converting to MP4…")
    subprocess.run(
        [
            "ffmpeg", "-y",
            "-i", str(WEBM),
            "-vf", "fps=30,scale=1440:900:flags=lanczos",
            "-c:v", "libx264",
            "-preset", "slow",
            "-crf", "18",
            "-pix_fmt", "yuv420p",
            str(MP4),
        ],
        check=True,
        capture_output=True,
    )
    WEBM.unlink()
    print(f"\nDone! Video saved to:\n  {MP4}")


if __name__ == "__main__":
    main()
