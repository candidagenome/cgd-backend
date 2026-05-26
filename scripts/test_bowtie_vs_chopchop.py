#!/usr/bin/env python3
"""
Test bowtie off-target search against CHOPCHOP expected guides for 20 genes.

This script runs the CRISPR guide designer with bowtie off-target search
and compares the results with CHOPCHOP expected guides.

Usage:
    python scripts/test_bowtie_vs_chopchop.py [--method METHOD]

    METHOD: blast, bruteforce, bowtie, or auto (default: bowtie)
"""
import argparse
import json
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from cgd.db.deps import get_db
from cgd.schemas.crispr_schema import (
    CrisprDesignRequest,
    OffTargetMethod,
    TargetRegion,
    PAMType,
)
from cgd.api.services.crispr_service import design_guides


def load_test_genes():
    """Load test genes from fixture file."""
    fixture_path = Path(__file__).parent.parent / "tests/api/fixtures/crispr_test_genes.json"
    with open(fixture_path) as f:
        return json.load(f)


def run_comparison(method: str = "bowtie", verbose: bool = False):
    """Run CRISPR guide design and compare with CHOPCHOP results."""
    test_genes = load_test_genes()

    # Map method string to enum
    method_map = {
        "blast": OffTargetMethod.BLAST,
        "bruteforce": OffTargetMethod.BRUTEFORCE,
        "bowtie": OffTargetMethod.BOWTIE,
        "auto": OffTargetMethod.AUTO,
    }
    offtarget_method = method_map.get(method.lower(), OffTargetMethod.BOWTIE)

    print(f"\n{'='*70}")
    print(f"Testing CRISPR Guide Design with {method.upper()} off-target search")
    print(f"{'='*70}\n")

    results_summary = []
    total_expected = 0
    total_found = 0
    total_matched = 0

    # Get database session
    db = next(get_db())

    try:
        for gene_data in test_genes:
            gene_name = gene_data["gene_name"]
            expected_guides = set(gene_data["expected_guides_5prime"])
            total_expected += len(expected_guides)

            print(f"\n{'-'*50}")
            print(f"Gene: {gene_name} ({gene_data['feature_name']})")
            print(f"Expected CHOPCHOP guides (5' region): {len(expected_guides)}")

            # Create request
            request = CrisprDesignRequest(
                gene_name=gene_name,
                organism="C_albicans_SC5314_A22",
                pam=PAMType.NGG,
                guide_length=20,
                target_region=TargetRegion.FIVE_PRIME,
                check_offtargets=True,
                offtarget_method=offtarget_method,
                max_offtarget_mismatches=3,
                max_guides=50,
            )

            # Run guide design
            try:
                response = design_guides(db, request)

                if not response.success:
                    print(f"  ERROR: {response.error}")
                    results_summary.append({
                        "gene": gene_name,
                        "status": "error",
                        "error": response.error,
                    })
                    continue

                # Get generated guide sequences
                generated_guides = set(g.sequence for g in response.guides)
                total_found += len(generated_guides)

                # Calculate matches
                matched = expected_guides & generated_guides
                total_matched += len(matched)
                missing = expected_guides - generated_guides
                extra = generated_guides - expected_guides

                match_rate = len(matched) / len(expected_guides) * 100 if expected_guides else 0

                print(f"Generated guides: {len(generated_guides)}")
                print(f"Matched: {len(matched)}/{len(expected_guides)} ({match_rate:.1f}%)")

                if verbose:
                    if matched:
                        print(f"  Matched guides: {sorted(matched)[:5]}...")
                    if missing:
                        print(f"  Missing from CHOPCHOP: {sorted(missing)[:3]}...")
                    if extra:
                        print(f"  Extra guides (not in CHOPCHOP top 10): {len(extra)}")

                # Show warnings
                if response.warnings:
                    for w in response.warnings[:2]:
                        print(f"  Warning: {w}")

                # Show off-target stats for top guide
                if response.guides:
                    top_guide = response.guides[0]
                    print(f"  Top guide: {top_guide.sequence}")
                    print(f"    Off-target checked: {top_guide.offtarget_checked}")
                    print(f"    Off-targets: {top_guide.offtarget_count} "
                          f"(0mm:{top_guide.offtarget_0mm}, 1mm:{top_guide.offtarget_1mm}, "
                          f"2mm:{top_guide.offtarget_2mm}, 3mm:{top_guide.offtarget_3mm})")
                    print(f"    Combined score: {top_guide.combined_score}")

                results_summary.append({
                    "gene": gene_name,
                    "status": "success",
                    "expected": len(expected_guides),
                    "generated": len(generated_guides),
                    "matched": len(matched),
                    "match_rate": match_rate,
                    "offtarget_checked": response.guides[0].offtarget_checked if response.guides else False,
                })

            except Exception as e:
                print(f"  EXCEPTION: {e}")
                results_summary.append({
                    "gene": gene_name,
                    "status": "exception",
                    "error": str(e),
                })

    finally:
        db.close()

    # Print summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    print(f"Method: {method.upper()}")
    print(f"Total genes tested: {len(test_genes)}")
    print(f"Total expected guides: {total_expected}")
    print(f"Total generated guides: {total_found}")
    print(f"Total matched: {total_matched}")
    print(f"Overall match rate: {total_matched/total_expected*100:.1f}%")

    # Count successes
    successes = [r for r in results_summary if r["status"] == "success"]
    offtarget_checked = sum(1 for r in successes if r.get("offtarget_checked", False))
    print(f"Genes with off-target search completed: {offtarget_checked}/{len(successes)}")

    # Show any failures
    failures = [r for r in results_summary if r["status"] != "success"]
    if failures:
        print(f"\nFailures ({len(failures)}):")
        for f in failures:
            print(f"  {f['gene']}: {f.get('error', 'unknown error')}")

    return results_summary


def main():
    parser = argparse.ArgumentParser(
        description="Test CRISPR guide design with different off-target methods"
    )
    parser.add_argument(
        "--method", "-m",
        choices=["blast", "bruteforce", "bowtie", "auto"],
        default="bowtie",
        help="Off-target search method (default: bowtie)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed guide comparisons"
    )
    args = parser.parse_args()

    run_comparison(method=args.method, verbose=args.verbose)


if __name__ == "__main__":
    main()
