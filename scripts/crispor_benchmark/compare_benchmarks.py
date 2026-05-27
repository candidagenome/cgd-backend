#!/usr/bin/env python3
"""
Compare CRISPR guide rankings across CGD, CHOPCHOP, and CRISPOR.

This script performs TOP-10 vs TOP-10 comparisons as documented in the README:
1. CHOPCHOP top 10 found in CGD top 10
2. CRISPOR top 10 found in CGD top 10
3. CGD top 10 found in CHOPCHOP top 10
4. CGD top 10 found in CRISPOR top 10
5. Extended comparison: top 10 found in top 20

This measures RANKING ALIGNMENT, not just guide discovery.
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


def count_overlap(list_a: List[str], list_b: List[str], top_n: int = 10) -> int:
    """Count how many of list_a's top N appear in list_b's top N."""
    set_a = set(list_a[:top_n])
    set_b = set(list_b[:top_n])
    return len(set_a & set_b)


def analyze_gene(
    gene_name: str,
    chopchop_guides: List[str],
    crispor_guides: List[str],
    cgd_guides: List[str],
) -> dict:
    """Analyze top-10 vs top-10 overlap for a single gene."""
    # Top 10 sets
    chopchop_top10 = set(chopchop_guides[:10])
    crispor_top10 = set(crispor_guides[:10])
    cgd_top10 = set(cgd_guides[:10])
    cgd_top20 = set(cgd_guides[:20])

    # Top 10 vs Top 10 comparisons
    chopchop_in_cgd_top10 = len(chopchop_top10 & cgd_top10)
    crispor_in_cgd_top10 = len(crispor_top10 & cgd_top10)
    cgd_in_chopchop_top10 = len(cgd_top10 & chopchop_top10)
    cgd_in_crispor_top10 = len(cgd_top10 & crispor_top10)
    chopchop_in_crispor_top10 = len(chopchop_top10 & crispor_top10)
    crispor_in_chopchop_top10 = len(crispor_top10 & chopchop_top10)

    # Extended: Top 10 in Top 20
    chopchop_in_cgd_top20 = len(chopchop_top10 & cgd_top20)
    crispor_in_cgd_top20 = len(crispor_top10 & cgd_top20)

    # Consensus (all 3 tools agree in top 10)
    consensus_3 = chopchop_top10 & crispor_top10 & cgd_top10

    return {
        "gene_name": gene_name,
        "chopchop_count": min(len(chopchop_guides), 10),
        "crispor_count": min(len(crispor_guides), 10),
        "cgd_count": min(len(cgd_guides), 10),
        # Top 10 vs Top 10
        "chopchop_in_cgd_top10": chopchop_in_cgd_top10,
        "crispor_in_cgd_top10": crispor_in_cgd_top10,
        "cgd_in_chopchop_top10": cgd_in_chopchop_top10,
        "cgd_in_crispor_top10": cgd_in_crispor_top10,
        "chopchop_in_crispor_top10": chopchop_in_crispor_top10,
        # Extended
        "chopchop_in_cgd_top20": chopchop_in_cgd_top20,
        "crispor_in_cgd_top20": crispor_in_cgd_top20,
        # Consensus
        "consensus_3_tools": len(consensus_3),
        "consensus_guides": list(consensus_3),
        # Full guide lists for detailed comparison
        "chopchop_guides": chopchop_guides,
        "crispor_guides": crispor_guides,
        "cgd_guides": cgd_guides,
    }


def print_detailed_comparison(result: dict):
    """Print detailed ranking comparison for a gene."""
    gene_name = result["gene_name"]
    chopchop_guides = result["chopchop_guides"]
    crispor_guides = result["crispor_guides"]
    cgd_guides = result["cgd_guides"]

    chopchop_rank = {g: i+1 for i, g in enumerate(chopchop_guides)}
    crispor_rank = {g: i+1 for i, g in enumerate(crispor_guides)}
    cgd_rank = {g: i+1 for i, g in enumerate(cgd_guides)}

    # Show guides that appear in any tool's top 10
    all_top10 = set(chopchop_guides[:10]) | set(crispor_guides[:10]) | set(cgd_guides[:10])

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

    for guide in sorted(all_top10, key=sort_key):
        chop_r = chopchop_rank.get(guide, "-")
        crisp_r = crispor_rank.get(guide, "-")
        cgd_r = cgd_rank.get(guide, "-")

        # Highlight if in top 10 of each tool
        chop_str = str(chop_r) if chop_r == "-" or chop_r > 10 else f"*{chop_r}*"
        crisp_str = str(crisp_r) if crisp_r == "-" or crisp_r > 10 else f"*{crisp_r}*"
        cgd_str = str(cgd_r) if cgd_r == "-" or cgd_r > 10 else f"*{cgd_r}*"

        # Determine consensus level (all must be in top 10)
        in_top10 = sum([
            isinstance(chop_r, int) and chop_r <= 10,
            isinstance(crisp_r, int) and crisp_r <= 10,
            isinstance(cgd_r, int) and cgd_r <= 10,
        ])
        if in_top10 == 3:
            consensus = "*** ALL 3 ***"
        elif in_top10 == 2:
            consensus = "2 tools"
        else:
            consensus = ""

        print(f"{guide:<24} {chop_str:<10} {crisp_str:<10} {cgd_str:<10} {consensus}")


def main():
    print("=" * 100)
    print("CRISPR Guide Benchmark: CGD vs CHOPCHOP vs CRISPOR")
    print("=" * 100)
    print("Comparison method: TOP-10 vs TOP-10 (strict ranking alignment)")
    print("=" * 100)

    chopchop_data, crispor_data = load_fixtures()

    # Build lookup for CRISPOR data
    crispor_by_gene = {g["gene_name"]: g for g in crispor_data}

    results = []

    # Totals for summary
    total_chopchop_guides = 0
    total_crispor_guides = 0
    total_cgd_guides = 0

    total_chopchop_in_cgd_top10 = 0
    total_crispor_in_cgd_top10 = 0
    total_cgd_in_chopchop_top10 = 0
    total_cgd_in_crispor_top10 = 0
    total_chopchop_in_crispor_top10 = 0

    total_chopchop_in_cgd_top20 = 0
    total_crispor_in_cgd_top20 = 0

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

        # Accumulate totals
        total_chopchop_guides += analysis["chopchop_count"]
        total_crispor_guides += analysis["crispor_count"]
        total_cgd_guides += analysis["cgd_count"]

        total_chopchop_in_cgd_top10 += analysis["chopchop_in_cgd_top10"]
        total_crispor_in_cgd_top10 += analysis["crispor_in_cgd_top10"]
        total_cgd_in_chopchop_top10 += analysis["cgd_in_chopchop_top10"]
        total_cgd_in_crispor_top10 += analysis["cgd_in_crispor_top10"]
        total_chopchop_in_crispor_top10 += analysis["chopchop_in_crispor_top10"]

        total_chopchop_in_cgd_top20 += analysis["chopchop_in_cgd_top20"]
        total_crispor_in_cgd_top20 += analysis["crispor_in_cgd_top20"]

        total_consensus_3 += analysis["consensus_3_tools"]

    # Print per-gene summary
    print("\n" + "=" * 100)
    print("PER-GENE SUMMARY (Top 10 vs Top 10)")
    print("=" * 100)

    print(f"\n{'Gene':<10} {'CHOP':<6} {'CRISP':<6} {'CGD':<6} "
          f"{'CHOP→CGD':<10} {'CRISP→CGD':<10} {'All 3':<6}")
    print("-" * 70)
    for r in results:
        chop_cgd = f"{r['chopchop_in_cgd_top10']}/{r['chopchop_count']}"
        crisp_cgd = f"{r['crispor_in_cgd_top10']}/{r['crispor_count']}" if r['crispor_count'] > 0 else "-"
        print(f"{r['gene_name']:<10} {r['chopchop_count']:<6} {r['crispor_count']:<6} {r['cgd_count']:<6} "
              f"{chop_cgd:<10} {crisp_cgd:<10} {r['consensus_3_tools']:<6}")

    # Print main summary table (matching README format)
    print("\n" + "=" * 100)
    print("TOP 10 GUIDE COMPARISON (20 Genes)")
    print("=" * 100)

    print(f"\n{'Comparison':<45} {'Overlap':<12} {'Match Rate'}")
    print("-" * 70)

    # CHOPCHOP top 10 found in CGD top 10
    rate = 100 * total_chopchop_in_cgd_top10 / total_chopchop_guides if total_chopchop_guides > 0 else 0
    print(f"{'CHOPCHOP top 10 found in CGD top 10':<45} {total_chopchop_in_cgd_top10}/{total_chopchop_guides:<6} {rate:.1f}%")

    # CHOPCHOP top 10 found in CRISPOR top 10
    rate = 100 * total_chopchop_in_crispor_top10 / total_chopchop_guides if total_chopchop_guides > 0 else 0
    print(f"{'CHOPCHOP top 10 found in CRISPOR top 10':<45} {total_chopchop_in_crispor_top10}/{total_chopchop_guides:<6} {rate:.1f}%")

    # CRISPOR top 10 found in CGD top 10
    rate = 100 * total_crispor_in_cgd_top10 / total_crispor_guides if total_crispor_guides > 0 else 0
    print(f"{'CRISPOR top 10 found in CGD top 10':<45} {total_crispor_in_cgd_top10}/{total_crispor_guides:<6} {rate:.1f}%")

    # CGD top 10 found in CRISPOR top 10
    rate = 100 * total_cgd_in_crispor_top10 / total_cgd_guides if total_cgd_guides > 0 else 0
    print(f"{'CGD top 10 found in CRISPOR top 10':<45} {total_cgd_in_crispor_top10}/{total_cgd_guides:<6} {rate:.1f}%")

    # CGD top 10 found in CHOPCHOP top 10
    rate = 100 * total_cgd_in_chopchop_top10 / total_cgd_guides if total_cgd_guides > 0 else 0
    print(f"{'CGD top 10 found in CHOPCHOP top 10':<45} {total_cgd_in_chopchop_top10}/{total_cgd_guides:<6} {rate:.1f}%")

    # Extended comparison
    print("\n" + "=" * 100)
    print("EXTENDED COMPARISON (Top 10 in Top 20)")
    print("=" * 100)

    print(f"\n{'Comparison':<45} {'Match Rate'}")
    print("-" * 70)

    rate = 100 * total_chopchop_in_cgd_top20 / total_chopchop_guides if total_chopchop_guides > 0 else 0
    print(f"{'CHOPCHOP top 10 found in CGD top 20':<45} {rate:.1f}% ({total_chopchop_in_cgd_top20}/{total_chopchop_guides})")

    rate = 100 * total_crispor_in_cgd_top20 / total_crispor_guides if total_crispor_guides > 0 else 0
    print(f"{'CRISPOR top 10 found in CGD top 20':<45} {rate:.1f}% ({total_crispor_in_cgd_top20}/{total_crispor_guides})")

    print(f"\nConsensus guides (in all 3 tools' top 10): {total_consensus_3}")

    # Print detailed comparison for select genes
    print("\n" + "=" * 100)
    print("DETAILED RANKING COMPARISON (showing guides in any tool's top 10)")
    print("*N* = guide is in that tool's top 10")
    print("=" * 100)

    for result in results[:5]:  # First 5 genes for detail
        print_detailed_comparison(result)


if __name__ == "__main__":
    main()
