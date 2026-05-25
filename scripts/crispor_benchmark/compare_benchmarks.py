#!/usr/bin/env python3
"""
Compare CRISPR guide rankings across CGD, CHOPCHOP, and CRISPOR.

This script generates a comprehensive comparison report showing:
1. Guide overlap between tools (consensus guides)
2. Ranking correlation analysis
3. Per-gene breakdown of matches/misses
4. Summary statistics
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def load_fixtures() -> Tuple[List[dict], List[dict]]:
    """Load CHOPCHOP and CRISPOR fixture data."""
    fixtures_dir = Path(__file__).parent.parent.parent / "tests/api/fixtures"

    with open(fixtures_dir / "crispr_test_genes.json") as f:
        chopchop_data = json.load(f)

    crispor_path = Path(__file__).parent / "crispor_results.json"
    if crispor_path.exists():
        with open(crispor_path) as f:
            crispor_data = json.load(f)
    else:
        print(f"WARNING: CRISPOR results not found at {crispor_path}")
        print("Using template - run CRISPOR analysis first and save results.\n")
        with open(Path(__file__).parent / "crispor_results_template.json") as f:
            crispor_data = json.load(f)

    return chopchop_data, crispor_data


def get_cgd_guides(gene_name: str, max_guides: int = 50) -> List[str]:
    """Get CGD-generated guides for a gene."""
    from cgd.db.engine import SessionLocal
    from cgd.api.services import crispr_service
    from cgd.schemas.crispr_schema import CrisprDesignRequest, OffTargetMethod

    db = SessionLocal()
    try:
        request = CrisprDesignRequest(
            gene_name=gene_name,
            organism="C_albicans_SC5314_A22",
            pam_type="NGG",
            target_region="5_prime",
            guide_length=20,
            max_guides=max_guides,
            offtarget_method=OffTargetMethod.BOWTIE,
        )
        result = crispr_service.design_guides(db, request)
        return [g.sequence for g in result.guides]
    finally:
        db.close()


def analyze_gene(
    gene_name: str,
    chopchop_guides: List[str],
    crispor_guides: List[str],
    cgd_guides: List[str],
) -> dict:
    """Analyze guide overlap and rankings for a single gene."""
    chopchop_set = set(chopchop_guides)
    crispor_set = set(crispor_guides)
    cgd_set = set(cgd_guides)

    # Create rank lookups
    chopchop_rank = {g: i+1 for i, g in enumerate(chopchop_guides)}
    crispor_rank = {g: i+1 for i, g in enumerate(crispor_guides)}
    cgd_rank = {g: i+1 for i, g in enumerate(cgd_guides)}

    # Find overlaps
    all_guides = chopchop_set | crispor_set | cgd_set
    consensus_2 = (chopchop_set & crispor_set) | (chopchop_set & cgd_set) | (crispor_set & cgd_set)
    consensus_3 = chopchop_set & crispor_set & cgd_set

    # CGD matches
    cgd_matches_chopchop = cgd_set & chopchop_set
    cgd_matches_crispor = cgd_set & crispor_set

    return {
        "gene_name": gene_name,
        "chopchop_count": len(chopchop_guides),
        "crispor_count": len(crispor_guides),
        "cgd_count": len(cgd_guides),
        "cgd_matches_chopchop": len(cgd_matches_chopchop),
        "cgd_matches_crispor": len(cgd_matches_crispor),
        "consensus_2_tools": len(consensus_2),
        "consensus_3_tools": len(consensus_3),
        "chopchop_rank": chopchop_rank,
        "crispor_rank": crispor_rank,
        "cgd_rank": cgd_rank,
        "consensus_guides": list(consensus_3),
    }


def print_detailed_comparison(
    gene_name: str,
    chopchop_guides: List[str],
    crispor_guides: List[str],
    cgd_guides: List[str],
):
    """Print detailed ranking comparison for a gene."""
    chopchop_rank = {g: i+1 for i, g in enumerate(chopchop_guides)}
    crispor_rank = {g: i+1 for i, g in enumerate(crispor_guides)}
    cgd_rank = {g: i+1 for i, g in enumerate(cgd_guides)}

    all_guides = set(chopchop_guides) | set(crispor_guides) | set(cgd_guides[:20])

    print(f"\n{gene_name}")
    print("-" * 100)
    print(f"{'Guide Sequence':<24} {'CHOPCHOP':<10} {'CRISPOR':<10} {'CGD':<10} {'Consensus'}")
    print("-" * 100)

    # Sort by CGD rank, then CHOPCHOP, then CRISPOR
    def sort_key(g):
        return (
            cgd_rank.get(g, 999),
            chopchop_rank.get(g, 999),
            crispor_rank.get(g, 999),
        )

    for guide in sorted(all_guides, key=sort_key)[:20]:
        chop_r = chopchop_rank.get(guide, "-")
        crisp_r = crispor_rank.get(guide, "-")
        cgd_r = cgd_rank.get(guide, "-")

        # Determine consensus level
        in_tools = sum([
            guide in chopchop_rank,
            guide in crispor_rank,
            guide in cgd_rank,
        ])
        if in_tools == 3:
            consensus = "*** ALL 3 ***"
        elif in_tools == 2:
            consensus = "2 tools"
        else:
            consensus = ""

        print(f"{guide:<24} {str(chop_r):<10} {str(crisp_r):<10} {str(cgd_r):<10} {consensus}")


def main():
    print("=" * 100)
    print("CRISPR Guide Benchmark: CGD vs CHOPCHOP vs CRISPOR")
    print("=" * 100)

    chopchop_data, crispor_data = load_fixtures()

    # Build lookup for CRISPOR data
    crispor_by_gene = {g["gene_name"]: g for g in crispor_data}

    results = []
    total_chopchop = 0
    total_crispor = 0
    total_cgd_match_chopchop = 0
    total_cgd_match_crispor = 0
    total_consensus_3 = 0

    for gene_data in chopchop_data:
        gene_name = gene_data["gene_name"]
        chopchop_guides = gene_data["expected_guides_5prime"]

        # Get CRISPOR guides
        crispor_gene = crispor_by_gene.get(gene_name, {})
        crispor_guides = [g["sequence"] for g in crispor_gene.get("crispor_top_guides", [])]

        # Get CGD guides
        print(f"Processing {gene_name}...", end=" ", flush=True)
        try:
            cgd_guides = get_cgd_guides(gene_name)
            print(f"got {len(cgd_guides)} guides")
        except Exception as e:
            print(f"ERROR: {e}")
            cgd_guides = []

        # Analyze
        analysis = analyze_gene(gene_name, chopchop_guides, crispor_guides, cgd_guides)
        results.append(analysis)

        total_chopchop += len(chopchop_guides)
        total_crispor += len(crispor_guides)
        total_cgd_match_chopchop += analysis["cgd_matches_chopchop"]
        total_cgd_match_crispor += analysis["cgd_matches_crispor"]
        total_consensus_3 += analysis["consensus_3_tools"]

    # Print summary
    print("\n" + "=" * 100)
    print("SUMMARY")
    print("=" * 100)

    print(f"\n{'Gene':<10} {'CHOPCHOP':<10} {'CRISPOR':<10} {'CGD→CHOP':<12} {'CGD→CRISP':<12} {'All 3'}")
    print("-" * 70)
    for r in results:
        chop_match = f"{r['cgd_matches_chopchop']}/{r['chopchop_count']}"
        crisp_match = f"{r['cgd_matches_crispor']}/{r['crispor_count']}" if r['crispor_count'] > 0 else "-"
        print(f"{r['gene_name']:<10} {r['chopchop_count']:<10} {r['crispor_count']:<10} {chop_match:<12} {crisp_match:<12} {r['consensus_3_tools']}")

    print("-" * 70)
    chopchop_pct = 100 * total_cgd_match_chopchop / total_chopchop if total_chopchop > 0 else 0
    crispor_pct = 100 * total_cgd_match_crispor / total_crispor if total_crispor > 0 else 0

    print(f"\nCGD vs CHOPCHOP match rate: {total_cgd_match_chopchop}/{total_chopchop} ({chopchop_pct:.1f}%)")
    if total_crispor > 0:
        print(f"CGD vs CRISPOR match rate:  {total_cgd_match_crispor}/{total_crispor} ({crispor_pct:.1f}%)")
    print(f"Consensus (all 3 tools):    {total_consensus_3} guides")

    # Print detailed comparison for select genes
    print("\n" + "=" * 100)
    print("DETAILED RANKING COMPARISON (showing top 20 guides)")
    print("=" * 100)

    for gene_data in chopchop_data[:5]:  # First 5 genes for detail
        gene_name = gene_data["gene_name"]
        chopchop_guides = gene_data["expected_guides_5prime"]
        crispor_gene = crispor_by_gene.get(gene_name, {})
        crispor_guides = [g["sequence"] for g in crispor_gene.get("crispor_top_guides", [])]

        try:
            cgd_guides = get_cgd_guides(gene_name)
        except Exception:
            cgd_guides = []

        print_detailed_comparison(gene_name, chopchop_guides, crispor_guides, cgd_guides)


if __name__ == "__main__":
    main()
