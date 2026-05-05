#!/usr/bin/env python3
"""
Expression Data Proof-of-Concept
Extracts expression values for a gene across RNA-seq conditions.
"""
import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pyBigWig
from cgd.db.engine import SessionLocal
from cgd.models.models import Feature, Seq, FeatLocation

# Configuration
HTS_BASE = "/data/HTS/C_albicans_SC5314/bam"
ORGANISM = "C_albicans_SC5314"

# Study metadata with condition categories
STUDIES = {
    "Bruno_2010": {
        "category": "Stress Response",
        "conditions": {
            "nOxi": {"label": "Control (no oxidative stress)", "bucket": "control"},
            "lOxi": {"label": "Low H2O2 (0.5mM)", "bucket": "stress"},
            "hOxi": {"label": "High H2O2 (5mM)", "bucket": "stress"},
            "nitroSm": {"label": "Nitrosative control", "bucket": "control"},
            "nitroSp": {"label": "Nitrosative stress", "bucket": "stress"},
            "cwdm": {"label": "Cell wall control", "bucket": "control"},
            "cwdp": {"label": "Cell wall damage (congo red)", "bucket": "stress"},
            "ph4": {"label": "pH 4 (acidic)", "bucket": "basic_biology"},
            "ph8": {"label": "pH 8 (alkaline)", "bucket": "basic_biology"},
            "ypd_ss": {"label": "YPD control", "bucket": "control"},
            "ypd_serum_ss": {"label": "Serum", "bucket": "basic_biology"},
        },
        "pmid": "20543895"
    },
    "Desai_2013": {
        "category": "Biofilm",
        "conditions": {
            "sc_plnk": {"label": "SC5314 planktonic", "bucket": "basic_biology"},
            "sc_film": {"label": "SC5314 biofilm", "bucket": "basic_biology"},
            "woW_plnk": {"label": "WO1-white planktonic", "bucket": "basic_biology"},
            "woW_film": {"label": "WO1-white biofilm", "bucket": "basic_biology"},
            "woO_plnk": {"label": "WO1-opaque planktonic", "bucket": "basic_biology"},
            "woO_film": {"label": "WO1-opaque biofilm", "bucket": "basic_biology"},
        },
        "pmid": "24307631"
    },
    "Niemiec_2017": {
        "category": "Immune Response",
        "conditions": {
            "chg_77": {"label": "PMN Control Yeast 0-min", "bucket": "control"},
            "chg_71": {"label": "PMN Yeast 15-min", "bucket": "kill_candida"},
            "chg_72": {"label": "PMN Yeast 30-min", "bucket": "kill_candida"},
            "chg_73": {"label": "PMN Yeast 60-min", "bucket": "kill_candida"},
            "chg_75": {"label": "PMN Control Hyphae 0-min", "bucket": "control"},
            "chg_79": {"label": "PMN Hyphae 15-min", "bucket": "kill_candida"},
            "chg_80": {"label": "PMN Hyphae 30-min", "bucket": "kill_candida"},
            "chg_81": {"label": "PMN Hyphae 60-min", "bucket": "kill_candida"},
            "chg_76": {"label": "NET Control Yeast", "bucket": "control"},
            "chg_70": {"label": "NETs Yeast", "bucket": "kill_candida"},
            "chg_74": {"label": "NET Control Hyphae", "bucket": "control"},
            "chg_69": {"label": "NETs Hyphae", "bucket": "kill_candida"},
        },
        "pmid": "28874114"
    },
    "Xie_2013": {
        "category": "Cell Type",
        "conditions": {
            "CY110wh": {"label": "White cells", "bucket": "basic_biology"},
            "CY110op": {"label": "Opaque cells", "bucket": "basic_biology"},
        },
        "pmid": "23326225"
    },
    "Lohse_2016": {
        "category": "Cell Type",
        "conditions": {
            "wt_con": {"label": "WT control", "bucket": "control"},
            "wt_gfp": {"label": "WT GFP", "bucket": "basic_biology"},
            "op_con": {"label": "Opaque control", "bucket": "control"},
            "op_gfp": {"label": "Opaque GFP", "bucket": "basic_biology"},
        },
        "pmid": "27622382"
    },
}

# Bucket categorization
BUCKET_LABELS = {
    "control": "Control",
    "basic_biology": "Basic Candida Biology",
    "kill_candida": "How to Kill Candida (Immune/Drug)",
    "stress": "Stress Response (Gray Area)",
}


def get_gene_coordinates(gene_name: str) -> dict:
    """Get gene coordinates from database."""
    db = SessionLocal()
    try:
        # Find the feature
        feature = db.query(Feature).filter(Feature.gene_name == gene_name).first()
        if not feature:
            # Try feature_name
            feature = db.query(Feature).filter(Feature.feature_name == gene_name).first()

        if not feature:
            return None

        # Get location on Ca22 chromosome (HapA)
        location = (
            db.query(FeatLocation)
            .join(Seq, FeatLocation.root_seq_no == Seq.seq_no)
            .join(Feature, Seq.feature_no == Feature.feature_no)
            .filter(
                FeatLocation.feature_no == feature.feature_no,
                FeatLocation.is_loc_current == "Y",
                Feature.feature_name.like("Ca22%"),
                Feature.feature_type == "chromosome"
            )
            .first()
        )

        if not location:
            return None

        # Get chromosome name
        root_seq = db.query(Seq).filter(Seq.seq_no == location.root_seq_no).first()
        root_feature = db.query(Feature).filter(Feature.feature_no == root_seq.feature_no).first()

        return {
            "gene_name": feature.gene_name or feature.feature_name,
            "feature_name": feature.feature_name,
            "chromosome": root_feature.feature_name,
            "start": location.start_coord,
            "end": location.stop_coord,
            "strand": location.strand,
        }
    finally:
        db.close()


def get_bigwig_path(study: str, condition: str, haplotype: str = "HapA") -> str:
    """Construct path to bigwig file."""
    return f"{HTS_BASE}/{study}/{haplotype}/{condition}/sorted_hits_bam2wig/sorted_hits.bigwig"


def get_expression_value(bigwig_path: str, chrom: str, start: int, end: int) -> float:
    """Extract mean coverage from bigwig file for a region."""
    try:
        bw = pyBigWig.open(bigwig_path)
        if bw is None:
            return None

        # pyBigWig uses 0-based coordinates
        stats = bw.stats(chrom, start - 1, end, type="mean")
        bw.close()

        if stats and stats[0] is not None:
            return round(stats[0], 2)
        return 0.0
    except Exception as e:
        return None


def analyze_gene_expression(gene_name: str) -> dict:
    """Analyze expression for a gene across all conditions."""
    coords = get_gene_coordinates(gene_name)
    if not coords:
        return {"error": f"Gene {gene_name} not found"}

    results = {
        "gene": coords,
        "expression": {},
        "by_bucket": {},
    }

    for bucket in BUCKET_LABELS:
        results["by_bucket"][bucket] = []

    for study_name, study_info in STUDIES.items():
        for condition, cond_info in study_info["conditions"].items():
            bigwig_path = get_bigwig_path(study_name, condition)

            if not os.path.exists(bigwig_path):
                continue

            value = get_expression_value(
                bigwig_path,
                coords["chromosome"],
                coords["start"],
                coords["end"]
            )

            if value is not None:
                entry = {
                    "study": study_name,
                    "condition": condition,
                    "label": cond_info["label"],
                    "category": study_info["category"],
                    "value": value,
                    "pmid": study_info.get("pmid"),
                }
                results["expression"][f"{study_name}_{condition}"] = entry
                results["by_bucket"][cond_info["bucket"]].append(entry)

    return results


def print_results(results: dict):
    """Print results in a readable format."""
    if "error" in results:
        print(f"Error: {results['error']}")
        return

    gene = results["gene"]
    print(f"\n{'='*70}")
    print(f"Expression Analysis for {gene['gene_name']} ({gene['feature_name']})")
    print(f"Location: {gene['chromosome']}:{gene['start']}-{gene['end']} ({gene['strand']})")
    print(f"{'='*70}")

    # Print by bucket
    for bucket, label in BUCKET_LABELS.items():
        entries = results["by_bucket"].get(bucket, [])
        if not entries:
            continue

        print(f"\n## {label}")
        print("-" * 60)

        # Sort by value descending
        entries.sort(key=lambda x: x["value"], reverse=True)

        for entry in entries:
            print(f"  {entry['value']:8.2f}  {entry['label'][:40]:<40} ({entry['study']})")

    # Summary statistics
    all_values = [e["value"] for e in results["expression"].values()]
    if all_values:
        print(f"\n{'='*70}")
        print("Summary Statistics")
        print(f"  Min: {min(all_values):.2f}")
        print(f"  Max: {max(all_values):.2f}")
        print(f"  Mean: {sum(all_values)/len(all_values):.2f}")
        print(f"  Conditions analyzed: {len(all_values)}")


if __name__ == "__main__":
    # Test with HOG1 (stress response gene)
    gene = sys.argv[1] if len(sys.argv) > 1 else "HOG1"

    print(f"Analyzing expression for gene: {gene}")
    results = analyze_gene_expression(gene)
    print_results(results)
