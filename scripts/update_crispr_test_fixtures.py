#!/usr/bin/env python3
"""
Update CRISPR test fixtures with CHOPCHOP guide sequences.

This script reads the fixture JSON file (after you've added expected guides)
and generates Python code to paste into test_crispr_service.py.

Usage:
    # After adding expected_guides_5prime to crispr_test_genes.json:
    python scripts/update_crispr_test_fixtures.py

The script will output Python code that can be copied into the test file.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

# Fixture file path
FIXTURE_FILE = Path(__file__).parent.parent / "tests" / "api" / "fixtures" / "crispr_test_genes.json"


def main():
    """Generate Python fixture code from JSON."""
    if not FIXTURE_FILE.exists():
        print(f"ERROR: Fixture file not found: {FIXTURE_FILE}")
        print("Run fetch_crispr_test_fixtures.py first to generate the file.")
        sys.exit(1)

    with open(FIXTURE_FILE) as f:
        genes = json.load(f)

    # Check if any genes have expected guides
    genes_with_guides = [g for g in genes if g.get("expected_guides_5prime")]

    if not genes_with_guides:
        print("WARNING: No genes have expected_guides_5prime populated.")
        print("")
        print("To add expected guides:")
        print("1. Open tests/api/fixtures/crispr_test_genes.json")
        print("2. For each gene, add guide sequences to 'expected_guides_5prime'")
        print("   Example:")
        print('   "expected_guides_5prime": [')
        print('       "ATGCGATCGATCGATCGATC",')
        print('       "GCTAGCTAGCTAGCTAGCTA"')
        print('   ]')
        print("")
        print("Guide sequences should be 20bp, without the PAM.")
        print("")

    # Generate Python code
    print("# " + "="*70)
    print("# CRISPR Test Gene Fixtures")
    print("# Generated from tests/api/fixtures/crispr_test_genes.json")
    print("# " + "="*70)
    print("")
    print("CRISPR_TEST_GENES = [")

    for gene in genes:
        print("    {")
        print(f'        "gene_name": "{gene["gene_name"]}",')
        print(f'        "feature_name": "{gene["feature_name"]}",')
        print(f'        "description": "{gene["description"]}",')

        # CDS sequence (truncate for readability if very long)
        cds = gene.get("cds_first_500bp", "")
        if len(cds) > 100:
            # Store full sequence but show truncated in comment
            print(f'        "cds_first_500bp": "{cds}",  # {len(cds)}bp')
        else:
            print(f'        "cds_first_500bp": "{cds}",')

        # Expected guides
        guides = gene.get("expected_guides_5prime", [])
        if guides:
            print(f'        "expected_guides_5prime": [')
            for guide in guides:
                print(f'            "{guide}",')
            print('        ],')
        else:
            print('        "expected_guides_5prime": [],  # TODO: Add from CHOPCHOP')

        print("    },")

    print("]")
    print("")

    # Summary
    print("")
    print("# " + "-"*70)
    print(f"# Summary: {len(genes)} genes, {len(genes_with_guides)} with expected guides")
    print("# " + "-"*70)

    if genes_with_guides:
        print("")
        print("# Genes with expected guides:")
        for g in genes_with_guides:
            print(f"#   - {g['gene_name']}: {len(g['expected_guides_5prime'])} guides")


def validate_guides():
    """Validate guide sequences in the fixture file."""
    if not FIXTURE_FILE.exists():
        return

    with open(FIXTURE_FILE) as f:
        genes = json.load(f)

    issues = []

    for gene in genes:
        gene_name = gene["gene_name"]
        guides = gene.get("expected_guides_5prime", [])

        for i, guide in enumerate(guides):
            # Check length
            if len(guide) != 20:
                issues.append(f"{gene_name} guide {i+1}: Length {len(guide)} (expected 20)")

            # Check valid bases
            invalid_bases = set(guide.upper()) - set("ACGT")
            if invalid_bases:
                issues.append(f"{gene_name} guide {i+1}: Invalid bases {invalid_bases}")

            # Check for PAM accidentally included
            if guide.upper().endswith("GG"):
                issues.append(f"{gene_name} guide {i+1}: May include PAM (ends with GG)")

    if issues:
        print("\n# VALIDATION ISSUES:")
        for issue in issues:
            print(f"#   WARNING: {issue}")


if __name__ == "__main__":
    main()
    validate_guides()
