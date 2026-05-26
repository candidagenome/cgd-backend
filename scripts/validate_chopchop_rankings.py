#!/usr/bin/env python3
"""
Validate CGD CRISPR guide rankings against CHOPCHOP expected guides.

This script compares CGD's CRISPR guide rankings with CHOPCHOP's rankings
for 20 benchmark genes to measure alignment between the two tools.

Usage:
    # Set up SSH port forwarding to dev server first:
    ssh -f -N -L 8000:localhost:8000 cgd-backend-dev

    # Run validation:
    python scripts/validate_chopchop_rankings.py

    # Or specify a different API URL:
    python scripts/validate_chopchop_rankings.py --api-url http://localhost:8080/api/crispr/design

Metrics reported:
    - Top 5 overlap: How many of CHOPCHOP's top 5 guides are in CGD's top 5
    - CHOPCHOP #1 in CGD Top 5/10: Whether CHOPCHOP's best guide ranks highly in CGD
    - CGD #1 in CHOPCHOP Top 10: Whether CGD's best guide ranks highly in CHOPCHOP
    - Any CGD Top 5 in CHOPCHOP Top 10: Whether any CGD top guide is good in CHOPCHOP
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: requests module not found. Install with: pip install requests")
    sys.exit(1)

# Paths
SCRIPT_DIR = Path(__file__).parent
FIXTURE_FILE = SCRIPT_DIR.parent / "tests" / "api" / "fixtures" / "crispr_test_genes.json"

# Default API URL (assumes SSH port forwarding is set up)
DEFAULT_API_URL = "http://localhost:8000/api/crispr/design"


def validate_rankings(api_url: str, verbose: bool = True) -> dict:
    """
    Validate CGD rankings against CHOPCHOP for all test genes.

    Args:
        api_url: URL of the CRISPR design API endpoint
        verbose: Print per-gene results

    Returns:
        Dictionary with summary metrics
    """
    if not FIXTURE_FILE.exists():
        print(f"ERROR: Fixture file not found: {FIXTURE_FILE}")
        sys.exit(1)

    with open(FIXTURE_FILE) as f:
        genes = json.load(f)

    results = []

    for gene in genes:
        gene_name = gene["gene_name"]
        sequence = gene.get("cds_first_500bp", "")
        chopchop_guides = gene.get("expected_guides_5prime", [])

        if not sequence or not chopchop_guides:
            if verbose:
                print(f"{gene_name}: Skipped (missing data)")
            continue

        # Query API using sequence (to match CHOPCHOP input exactly)
        try:
            resp = requests.post(api_url, json={
                "sequence": sequence,
                "organism": "C_albicans_SC5314_A22",
                "pam": "NGG",
                "guide_length": 20,
                "check_offtargets": True,
                "max_guides": 50,
            }, timeout=120)
        except requests.exceptions.ConnectionError:
            print(f"ERROR: Cannot connect to API at {api_url}")
            print("Make sure the server is running and port forwarding is set up:")
            print("  ssh -f -N -L 8000:localhost:8000 cgd-backend-dev")
            sys.exit(1)
        except requests.exceptions.Timeout:
            print(f"{gene_name}: API timeout")
            continue

        if resp.status_code != 200:
            print(f"{gene_name}: API error {resp.status_code}")
            continue

        data = resp.json()
        if not data.get("success"):
            print(f"{gene_name}: {data.get('error')}")
            continue

        cgd_guides = [g["sequence"] for g in data["guides"]]

        # Calculate metrics
        chopchop_top1 = chopchop_guides[0] if chopchop_guides else None
        cgd_top1 = cgd_guides[0] if cgd_guides else None

        chopchop_top5 = set(chopchop_guides[:5])
        cgd_top5 = set(cgd_guides[:5])
        cgd_top10 = set(cgd_guides[:10])
        chopchop_top10 = set(chopchop_guides[:10])

        overlap_5 = len(chopchop_top5 & cgd_top5)
        chopchop1_in_cgd5 = chopchop_top1 in cgd_top5 if chopchop_top1 else False
        chopchop1_in_cgd10 = chopchop_top1 in cgd_top10 if chopchop_top1 else False
        cgd1_in_chopchop10 = cgd_top1 in chopchop_top10 if cgd_top1 else False
        any_cgd5_in_chopchop10 = len(cgd_top5 & chopchop_top10) > 0

        # Find rank of CHOPCHOP #1 in CGD results
        chopchop1_cgd_rank = None
        if chopchop_top1 and chopchop_top1 in cgd_guides:
            chopchop1_cgd_rank = cgd_guides.index(chopchop_top1) + 1

        results.append({
            "gene": gene_name,
            "overlap_5": overlap_5,
            "chopchop1_in_cgd5": chopchop1_in_cgd5,
            "chopchop1_in_cgd10": chopchop1_in_cgd10,
            "cgd1_in_chopchop10": cgd1_in_chopchop10,
            "any_cgd5_in_chopchop10": any_cgd5_in_chopchop10,
            "chopchop1_cgd_rank": chopchop1_cgd_rank,
            "cgd_guides": len(cgd_guides),
        })

        if verbose:
            print(f"{gene_name}: Top5 overlap={overlap_5}/5, CHOPCHOP#1 CGD rank={chopchop1_cgd_rank}")

    # Calculate summary
    n = len(results)
    if n == 0:
        return {"error": "No results"}

    summary = {
        "genes_tested": n,
        "avg_top5_overlap": sum(r["overlap_5"] for r in results) / n,
        "chopchop1_in_cgd5_pct": sum(r["chopchop1_in_cgd5"] for r in results) / n * 100,
        "chopchop1_in_cgd10_pct": sum(r["chopchop1_in_cgd10"] for r in results) / n * 100,
        "cgd1_in_chopchop10_pct": sum(r["cgd1_in_chopchop10"] for r in results) / n * 100,
        "any_cgd5_in_chopchop10_pct": sum(r["any_cgd5_in_chopchop10"] for r in results) / n * 100,
        "per_gene": results,
    }

    # CHOPCHOP #1 rank distribution
    ranks = [r["chopchop1_cgd_rank"] for r in results if r["chopchop1_cgd_rank"]]
    if ranks:
        summary["chopchop1_rank_min"] = min(ranks)
        summary["chopchop1_rank_max"] = max(ranks)
        summary["chopchop1_rank_avg"] = sum(ranks) / len(ranks)
        summary["chopchop1_found_pct"] = len(ranks) / n * 100

    return summary


def print_summary(summary: dict) -> None:
    """Print validation summary."""
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)

    if "error" in summary:
        print(summary["error"])
        return

    print(f"Genes tested: {summary['genes_tested']}")
    print(f"Avg Top 5 overlap: {summary['avg_top5_overlap']:.1f}/5 "
          f"({summary['avg_top5_overlap']/5*100:.0f}%)")
    print(f"CHOPCHOP #1 in CGD Top 5: {summary['chopchop1_in_cgd5_pct']:.0f}%")
    print(f"CHOPCHOP #1 in CGD Top 10: {summary['chopchop1_in_cgd10_pct']:.0f}%")
    print(f"CGD #1 in CHOPCHOP Top 10: {summary['cgd1_in_chopchop10_pct']:.0f}%")
    print(f"Any CGD Top 5 in CHOPCHOP Top 10: {summary['any_cgd5_in_chopchop10_pct']:.0f}%")

    if "chopchop1_rank_avg" in summary:
        print(f"\nCHOPCHOP #1 rank in CGD results:")
        print(f"  Min: {summary['chopchop1_rank_min']}, "
              f"Max: {summary['chopchop1_rank_max']}, "
              f"Avg: {summary['chopchop1_rank_avg']:.1f}")
        print(f"  Found: {summary['chopchop1_found_pct']:.0f}%")


def main():
    parser = argparse.ArgumentParser(
        description="Validate CGD CRISPR rankings against CHOPCHOP"
    )
    parser.add_argument(
        "--api-url",
        default=DEFAULT_API_URL,
        help=f"API URL (default: {DEFAULT_API_URL})"
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Only show summary, not per-gene results"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON"
    )
    args = parser.parse_args()

    summary = validate_rankings(args.api_url, verbose=not args.quiet)

    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        print_summary(summary)


if __name__ == "__main__":
    main()
