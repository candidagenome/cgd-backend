#!/usr/bin/env python3
"""
Fetch CRISPR guide predictions from CHOPCHOP website using Playwright.

This script automates submission of gene sequences to CHOPCHOP and
extracts the predicted guide sequences to update test fixtures.

Usage:
    # Install playwright first:
    pip install playwright
    playwright install chromium

    # Run the script:
    python scripts/fetch_chopchop_guides.py

    # Run for a single gene:
    python scripts/fetch_chopchop_guides.py --gene HOG1

Note: This uses web scraping which may break if CHOPCHOP changes their UI.
Consider using CHOPCHOP's command-line tool for more reliable results:
https://bitbucket.org/valenlab/chopchop/src/master/

References:
- CHOPCHOP v3: https://academic.oup.com/nar/article/47/W1/W171/5491735
- CHOPCHOP website: https://chopchop.cbu.uib.no/
"""
from __future__ import annotations

import json
import logging
import sys
import time
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
SCRIPT_DIR = Path(__file__).parent
FIXTURES_DIR = SCRIPT_DIR.parent / "tests" / "api" / "fixtures"
FIXTURE_FILE = FIXTURES_DIR / "crispr_test_genes.json"

# CHOPCHOP settings
CHOPCHOP_URL = "https://chopchop.cbu.uib.no/"
MAX_GUIDES_TO_FETCH = 10
WAIT_TIMEOUT = 60000  # 60 seconds


def load_fixture_data() -> List[Dict[str, Any]]:
    """Load existing fixture data."""
    if not FIXTURE_FILE.exists():
        raise FileNotFoundError(f"Fixture file not found: {FIXTURE_FILE}")

    with open(FIXTURE_FILE) as f:
        return json.load(f)


def save_fixture_data(data: List[Dict[str, Any]]) -> None:
    """Save updated fixture data."""
    with open(FIXTURE_FILE, "w") as f:
        json.dump(data, f, indent=2)
    logger.info(f"Saved fixture data to {FIXTURE_FILE}")


def fetch_chopchop_guides(
    sequence: str,
    gene_name: str,
    max_guides: int = MAX_GUIDES_TO_FETCH,
    headless: bool = True,
) -> List[str]:
    """
    Submit a sequence to CHOPCHOP and extract guide predictions.

    Args:
        sequence: DNA sequence (first 500bp of CDS)
        gene_name: Gene name for logging
        max_guides: Maximum number of guides to fetch
        headless: Run browser in headless mode

    Returns:
        List of 20bp guide sequences (without PAM)
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise ImportError(
            "Playwright is required. Install with:\n"
            "  pip install playwright\n"
            "  playwright install chromium"
        )

    guides = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()

        try:
            logger.info(f"[{gene_name}] Navigating to CHOPCHOP...")
            page.goto(CHOPCHOP_URL, timeout=WAIT_TIMEOUT)

            # Wait for page to load
            page.wait_for_load_state("networkidle")
            time.sleep(2)  # Extra wait for JS

            # Click "Paste Target" button to open sequence input
            logger.info(f"[{gene_name}] Clicking Paste Target button...")
            paste_button = page.locator("button:has-text('Paste Target')")
            paste_button.click()
            time.sleep(1)

            # Find the sequence textarea and paste sequence in FASTA format
            logger.info(f"[{gene_name}] Pasting sequence ({len(sequence)} bp)...")
            textarea = page.locator("textarea#fastaSeq")
            if not textarea.is_visible():
                textarea = page.locator("textarea").first
            textarea.fill(f">{gene_name}\n{sequence}")

            # Submit the form by clicking "Find Target Sites!"
            logger.info(f"[{gene_name}] Submitting to CHOPCHOP...")
            submit_button = page.locator("button:has-text('Find Target Sites!')")
            submit_button.click()

            # Wait for results - CHOPCHOP queues jobs and auto-refreshes
            logger.info(f"[{gene_name}] Waiting for results (job queued)...")

            # Wait for the results table to appear
            max_wait = 180  # 3 minutes max
            wait_interval = 10
            elapsed = 0

            while elapsed < max_wait:
                time.sleep(wait_interval)
                elapsed += wait_interval

                # Check page content
                content = page.content()

                # Check if results table is present (has "Target sequence" header)
                if "Target sequence" in content and "Efficiency" in content:
                    logger.info(f"[{gene_name}] Results ready! ({elapsed}s)")
                    break

                # Check if still processing
                if "being processed" in content or "Refreshing in" in content:
                    logger.info(f"[{gene_name}] Still processing... ({elapsed}s)")
                    # Page auto-refreshes, wait for it
                    page.wait_for_load_state("networkidle", timeout=WAIT_TIMEOUT)
                    continue

                # Check for error
                if "error" in content.lower() and "no results" in content.lower():
                    logger.warning(f"[{gene_name}] CHOPCHOP returned no results")
                    break

                # Unknown state - reload page
                logger.info(f"[{gene_name}] Checking status... ({elapsed}s)")
                page.reload()
                page.wait_for_load_state("networkidle", timeout=WAIT_TIMEOUT)

            if elapsed >= max_wait:
                logger.warning(f"[{gene_name}] Timeout waiting for results")

            time.sleep(2)  # Extra wait for JS to render

            # Take a screenshot of results for debugging
            screenshot_path = FIXTURES_DIR / f"chopchop_results_{gene_name}.png"
            page.screenshot(path=str(screenshot_path), full_page=True)
            logger.info(f"[{gene_name}] Results screenshot: {screenshot_path}")

            # Extract guide sequences from results table
            # CHOPCHOP displays guides in a table - look for sequence cells
            # The sequences are in cells with class 'sequence' or similar

            # Try multiple selectors for finding guide sequences
            selectors = [
                "table tbody tr td:nth-child(2)",  # Usually sequence is 2nd column
                "td.sequence",
                ".guideSequence",
                "table tr td code",
            ]

            for selector in selectors:
                cells = page.locator(selector).all()
                if cells:
                    logger.info(f"[{gene_name}] Found {len(cells)} cells with selector: {selector}")
                    for cell in cells[:max_guides * 2]:
                        text = cell.inner_text().strip().upper()
                        # Guide sequences are 20bp, possibly with PAM (NGG)
                        # Clean up the text
                        text = ''.join(c for c in text if c in 'ACGT')
                        if len(text) >= 20:
                            guide = text[:20]  # Take first 20bp (guide without PAM)
                            if guide not in guides and len(guide) == 20:
                                guides.append(guide)
                                if len(guides) >= max_guides:
                                    break
                    if guides:
                        break

            # Alternative: Try to find guides in the page content using regex
            if not guides:
                logger.warning(f"[{gene_name}] No guides found in table, trying page content...")
                content = page.content()
                import re
                # Look for 20bp sequences followed by NGG PAM
                pattern = r'([ACGT]{20})[ACGT]GG'
                matches = re.findall(pattern, content)
                # Also try finding sequences in JSON data
                json_pattern = r'"sequence"\s*:\s*"([ACGT]{20,23})"'
                json_matches = re.findall(json_pattern, content)
                for match in json_matches:
                    if len(match) >= 20:
                        guides.append(match[:20])

                all_matches = list(dict.fromkeys(matches + guides))[:max_guides]
                guides = all_matches

            logger.info(f"[{gene_name}] Found {len(guides)} guides")

        except Exception as e:
            logger.error(f"[{gene_name}] Error: {e}")
            # Take screenshot for debugging
            screenshot_path = FIXTURES_DIR / f"chopchop_error_{gene_name}.png"
            page.screenshot(path=str(screenshot_path), full_page=True)
            logger.info(f"[{gene_name}] Screenshot saved: {screenshot_path}")

        finally:
            browser.close()

    return guides


def main():
    parser = argparse.ArgumentParser(
        description="Fetch CRISPR guides from CHOPCHOP website"
    )
    parser.add_argument(
        "--gene",
        help="Process only this gene (by name)",
        default=None,
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        default=True,
        help="Run browser in headless mode (default: True)",
    )
    parser.add_argument(
        "--visible",
        action="store_true",
        help="Run browser in visible mode (for debugging)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't save results, just print them",
    )
    args = parser.parse_args()

    headless = not args.visible

    # Load fixture data
    logger.info("Loading fixture data...")
    fixture_data = load_fixture_data()

    # Filter to specific gene if requested
    if args.gene:
        genes_to_process = [
            g for g in fixture_data
            if g["gene_name"].upper() == args.gene.upper()
        ]
        if not genes_to_process:
            logger.error(f"Gene not found: {args.gene}")
            sys.exit(1)
    else:
        genes_to_process = fixture_data

    # Process each gene
    updated_count = 0
    for gene_data in genes_to_process:
        gene_name = gene_data["gene_name"]
        sequence = gene_data.get("cds_first_500bp", "")

        if not sequence:
            logger.warning(f"[{gene_name}] No sequence available, skipping")
            continue

        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {gene_name}")
        logger.info(f"{'='*60}")

        try:
            guides = fetch_chopchop_guides(
                sequence=sequence,
                gene_name=gene_name,
                headless=headless,
            )

            if guides:
                print(f"\n{gene_name} - Found {len(guides)} guides:")
                for i, guide in enumerate(guides, 1):
                    print(f"  {i}. {guide}")

                # Update fixture data
                gene_data["expected_guides_5prime"] = guides
                updated_count += 1
            else:
                logger.warning(f"[{gene_name}] No guides found")

        except Exception as e:
            logger.error(f"[{gene_name}] Failed: {e}")

        # Be nice to the server
        time.sleep(5)

    # Save updated fixture data
    if not args.dry_run and updated_count > 0:
        save_fixture_data(fixture_data)
        print(f"\n✓ Updated {updated_count} genes in {FIXTURE_FILE}")
    elif args.dry_run:
        print(f"\n(Dry run - no changes saved)")


if __name__ == "__main__":
    main()
