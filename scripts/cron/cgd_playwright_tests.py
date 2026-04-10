#!/usr/bin/env python3
"""
CGD Playwright Smoke Tests - Verify frontend pages render correctly.

This script uses Playwright to load pages in a headless browser and verify
that JavaScript-rendered content appears correctly.

Usage:
    python cgd_playwright_tests.py
    python cgd_playwright_tests.py --base-url https://www.candidagenome.org

Environment Variables:
    CGD_WEB_URL: Frontend base URL (default: http://localhost:3000)
    SLACK_WEBHOOK_URL: Slack webhook for failure notifications (optional)
    ENV_STATE: Environment label (prod/dev)

Prerequisites:
    pip install playwright
    playwright install chromium
"""

import argparse
import json
import os
import sys
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
except ImportError:
    print("Error: playwright not installed. Run: pip install playwright && playwright install chromium")
    sys.exit(1)


@dataclass
class TestResult:
    name: str
    url: str
    passed: bool
    error: Optional[str] = None


class SmokeTests:
    def __init__(self, base_url: str, timeout: int = 30000):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.results: list[TestResult] = []

    def check_page(
        self,
        name: str,
        path: str,
        wait_for_selector: str,
        expected_text: Optional[str] = None,
        expected_selectors: Optional[list[str]] = None,
    ) -> TestResult:
        """
        Load a page and verify content renders correctly.

        Args:
            name: Test name for reporting
            path: URL path (e.g., /locus/ACT1)
            wait_for_selector: CSS selector to wait for (indicates page loaded)
            expected_text: Text that should appear on the page
            expected_selectors: Additional selectors that should exist
        """
        url = f"{self.base_url}{path}"
        print(f"  Checking: {name:<40} ", end="", flush=True)

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context()
                page = context.new_page()

                # Navigate to page
                page.goto(url, timeout=self.timeout)

                # Wait for main content to render
                page.wait_for_selector(wait_for_selector, timeout=self.timeout)

                # Check for expected text
                if expected_text:
                    content = page.content()
                    if expected_text not in content:
                        result = TestResult(
                            name=name,
                            url=url,
                            passed=False,
                            error=f"Missing text: {expected_text}"
                        )
                        print(f"\033[91mFAIL\033[0m (missing: {expected_text})")
                        self.results.append(result)
                        browser.close()
                        return result

                # Check for expected selectors
                if expected_selectors:
                    for selector in expected_selectors:
                        if not page.query_selector(selector):
                            result = TestResult(
                                name=name,
                                url=url,
                                passed=False,
                                error=f"Missing element: {selector}"
                            )
                            print(f"\033[91mFAIL\033[0m (missing: {selector})")
                            self.results.append(result)
                            browser.close()
                            return result

                browser.close()
                result = TestResult(name=name, url=url, passed=True)
                print("\033[92mOK\033[0m")
                self.results.append(result)
                return result

        except PlaywrightTimeout as e:
            result = TestResult(name=name, url=url, passed=False, error=f"Timeout: {str(e)}")
            print(f"\033[91mFAIL\033[0m (timeout)")
            self.results.append(result)
            return result
        except Exception as e:
            result = TestResult(name=name, url=url, passed=False, error=str(e))
            print(f"\033[91mFAIL\033[0m ({str(e)[:50]})")
            self.results.append(result)
            return result

    def run_all_tests(self):
        """Run all smoke tests."""

        print("Frontend Page Checks (Playwright):")
        print("-" * 60)

        # Home page
        self.check_page(
            name="Home page",
            path="/",
            wait_for_selector=".home-page, .homepage, main, #root div",
            expected_text="Candida",
        )

        # Locus page - check that gene info renders
        self.check_page(
            name="Locus page (ACT1)",
            path="/locus/ACT1",
            wait_for_selector=".locus-page, .locus-header, h1",
            expected_text="ACT1",
            expected_selectors=[".tab-navigation, .tab-button, nav"],
        )

        # Locus page - GO tab
        self.check_page(
            name="Locus GO tab",
            path="/locus/ACT1?tab=go",
            wait_for_selector=".go-details, .go-annotations-container, .annotation-type-section",
            expected_text="GO Annotation",
        )

        # Locus page - Phenotype tab
        self.check_page(
            name="Locus Phenotype tab",
            path="/locus/ACT1?tab=phenotype",
            wait_for_selector=".phenotype-details, .phenotype-intro, .no-data",
            expected_text="phenotype",
        )

        # Locus page - Sequence tab
        self.check_page(
            name="Locus Sequence tab",
            path="/locus/ACT1?tab=sequence",
            wait_for_selector=".sequence-details, .organism-section, .no-data",
            expected_text="Sequence",
        )

        # GO term page
        self.check_page(
            name="GO term page",
            path="/go/GO:0008150",
            wait_for_selector="h1, .go-term-page, .definition-section, table",
            expected_text="GO:0008150",
        )

        # Feature search page
        self.check_page(
            name="Feature search page",
            path="/feature-search",
            wait_for_selector=".feature-search, .search-form, form, input",
            expected_text="Search",
        )

        # BLAST page
        self.check_page(
            name="BLAST page",
            path="/blast",
            wait_for_selector=".blast-page, .blast-form, form, textarea",
            expected_text="BLAST",
        )

        # Phenotype search page
        self.check_page(
            name="Phenotype search page",
            path="/phenotype-search",
            wait_for_selector=".phenotype-search, form, .search",
            expected_text="Phenotype",
        )

        # Reference page
        self.check_page(
            name="Reference page",
            path="/reference/8349105",
            wait_for_selector=".reference-page, .reference-details, main",
            expected_text="Reference",
        )

        print()

    def get_summary(self) -> dict:
        """Get test summary."""
        passed = sum(1 for r in self.results if r.passed)
        failed = sum(1 for r in self.results if not r.passed)
        return {
            "total": len(self.results),
            "passed": passed,
            "failed": failed,
            "failures": [
                {"name": r.name, "url": r.url, "error": r.error}
                for r in self.results if not r.passed
            ]
        }

    def print_summary(self):
        """Print test summary."""
        summary = self.get_summary()

        print("=" * 60)
        print("SMOKE TEST RESULTS")
        print("=" * 60)
        print()
        print(f"  {'Total:':<20} {summary['total']}")
        print(f"  {'Passed:':<20} {summary['passed']}")
        print(f"  {'Failed:':<20} {summary['failed']}")
        print()

        if summary['failures']:
            print("FAILED TESTS:")
            print("-" * 60)
            for f in summary['failures']:
                print(f"  ✗ {f['name']}")
                print(f"    URL: {f['url']}")
                print(f"    Error: {f['error']}")
            print()

        print("=" * 60)
        print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

    def send_slack_notification(self, webhook_url: str, env_label: str = "DEV"):
        """Send Slack notification on failure."""
        summary = self.get_summary()

        if summary['failed'] == 0:
            return

        failures_text = "\n".join(
            f"  ✗ {f['name']}: {f['error']}"
            for f in summary['failures']
        )

        message = f""":rotating_light: *CGD Playwright Smoke Test Failed ({env_label})*

*{summary['failed']} of {summary['total']} checks failed*

```
{failures_text}
```

Base URL: {self.base_url}"""

        data = json.dumps({"text": message}).encode('utf-8')
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={'Content-Type': 'application/json'}
        )
        try:
            urllib.request.urlopen(req)
            print("Slack notification sent.")
        except Exception as e:
            print(f"Failed to send Slack notification: {e}")


def main():
    parser = argparse.ArgumentParser(description="CGD Playwright Smoke Tests")
    parser.add_argument(
        "--base-url",
        default=os.environ.get("CGD_WEB_URL", "http://localhost:3000"),
        help="Frontend base URL"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30000,
        help="Page load timeout in milliseconds"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("CGD Playwright Smoke Tests")
    print("=" * 60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Base URL: {args.base_url}")
    print("=" * 60)
    print()

    tests = SmokeTests(base_url=args.base_url, timeout=args.timeout)
    tests.run_all_tests()
    tests.print_summary()

    # Send Slack notification on failure
    slack_webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if slack_webhook and tests.get_summary()['failed'] > 0:
        env_label = "PROD" if os.environ.get("ENV_STATE") in ("prod", "production") else "DEV"
        tests.send_slack_notification(slack_webhook, env_label)

    # Exit with failure code if any tests failed
    sys.exit(1 if tests.get_summary()['failed'] > 0 else 0)


if __name__ == "__main__":
    main()
