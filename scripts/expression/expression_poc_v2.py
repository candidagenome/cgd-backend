#!/usr/bin/env python3
"""
Expression Data Proof-of-Concept v2
- Downloads bigwig files via HTTP (with caching)
- Calculates fold changes within each study
"""
import os
import sys
import urllib.request
import hashlib
from pathlib import Path

import pyBigWig

# Configuration
HTS_BASE_URL = "https://frontend.dev.candidagenome.org/hts/C_albicans_SC5314/bam"
CACHE_DIR = Path("/tmp/hts_cache")

# Test genes with coordinates
GENES = {
    "HOG1": {"chr": "Ca22chr1A_C_albicans_SC5314", "start": 1533366, "end": 1534691, "desc": "Stress response kinase"},
    "ALS1": {"chr": "Ca22chr6A_C_albicans_SC5314", "start": 32667, "end": 36569, "desc": "Adhesin/virulence"},
    "ACT1": {"chr": "Ca22chr3A_C_albicans_SC5314", "start": 449217, "end": 450494, "desc": "Housekeeping"},
    "EFG1": {"chr": "Ca22chrRA_C_albicans_SC5314", "start": 753953, "end": 755713, "desc": "Morphology regulator"},
    "CDR1": {"chr": "Ca22chr3A_C_albicans_SC5314", "start": 197636, "end": 202135, "desc": "Drug efflux pump"},
}

# Studies with control conditions for fold-change calculation
STUDIES = {
    "Bruno_2010": {
        "category": "Stress Response",
        "pmid": "20543895",
        "control": "nOxi",  # Control for fold-change
        "conditions": {
            "nOxi": {"label": "Control (no stress)", "bucket": "control"},
            "lOxi": {"label": "Low H2O2 (0.5mM)", "bucket": "stress"},
            "hOxi": {"label": "High H2O2 (5mM)", "bucket": "stress"},
            "nitroSm": {"label": "Nitrosative control", "bucket": "control"},
            "nitroSp": {"label": "Nitrosative stress", "bucket": "stress"},
            "cwdm": {"label": "Cell wall control", "bucket": "control"},
            "cwdp": {"label": "Cell wall damage", "bucket": "stress"},
            "ph4": {"label": "pH 4 (acidic)", "bucket": "basic_biology"},
            "ph8": {"label": "pH 8 (alkaline)", "bucket": "basic_biology"},
            "ypd_ss": {"label": "YPD steady state", "bucket": "control"},
            "ypd_serum_ss": {"label": "Serum", "bucket": "basic_biology"},
        },
    },
    "Desai_2013": {
        "category": "Biofilm",
        "pmid": "24307631",
        "control": "sc_plnk",
        "conditions": {
            "sc_plnk": {"label": "Planktonic", "bucket": "control"},
            "sc_film": {"label": "Biofilm", "bucket": "basic_biology"},
        },
    },
    "Niemiec_2017": {
        "category": "Immune Response",
        "pmid": "28874114",
        "control": "chg_77",
        "conditions": {
            "chg_77": {"label": "PMN Control 0-min", "bucket": "control"},
            "chg_71": {"label": "PMN 15-min", "bucket": "kill_candida"},
            "chg_72": {"label": "PMN 30-min", "bucket": "kill_candida"},
            "chg_73": {"label": "PMN 60-min", "bucket": "kill_candida"},
        },
    },
    "Xie_2013": {
        "category": "Cell Type",
        "pmid": "23326225",
        "control": "CY110wh",
        "conditions": {
            "CY110wh": {"label": "White cells", "bucket": "control"},
            "CY110op": {"label": "Opaque cells", "bucket": "basic_biology"},
        },
    },
}

BUCKET_LABELS = {
    "control": "Control",
    "basic_biology": "Basic Candida Biology",
    "kill_candida": "How to Kill Candida",
    "stress": "Stress Response",
}


def get_cache_path(url: str) -> Path:
    """Get cached file path for a URL."""
    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
    filename = url.split("/")[-1]
    return CACHE_DIR / f"{url_hash}_{filename}"


def download_bigwig(study: str, condition: str, haplotype: str = "HapA") -> Path:
    """Download bigwig file if not cached."""
    url = f"{HTS_BASE_URL}/{study}/{haplotype}/{condition}/sorted_hits_bam2wig/sorted_hits.bigwig"
    cache_path = get_cache_path(url)

    if not cache_path.exists():
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        try:
            urllib.request.urlretrieve(url, cache_path)
        except Exception as e:
            return None

    return cache_path


def get_expression(bigwig_path: Path, chrom: str, start: int, end: int) -> float:
    """Get mean expression value for a region."""
    try:
        bw = pyBigWig.open(str(bigwig_path))
        stats = bw.stats(chrom, start - 1, end, type="mean")
        bw.close()
        return stats[0] if stats and stats[0] else 0.0
    except Exception:
        return None


def analyze_gene(gene_name: str) -> dict:
    """Analyze expression for a gene with fold-change calculation."""
    if gene_name not in GENES:
        return {"error": f"Gene {gene_name} not found in test set"}

    gene = GENES[gene_name]
    results = {
        "gene": gene_name,
        "description": gene["desc"],
        "location": f"{gene['chr']}:{gene['start']}-{gene['end']}",
        "studies": {},
    }

    for study_name, study_info in STUDIES.items():
        control_cond = study_info["control"]
        control_path = download_bigwig(study_name, control_cond)

        if not control_path:
            continue

        control_value = get_expression(control_path, gene["chr"], gene["start"], gene["end"])

        if control_value is None or control_value == 0:
            continue

        study_results = {
            "category": study_info["category"],
            "control": control_cond,
            "control_value": round(control_value, 2),
            "conditions": [],
        }

        for cond_name, cond_info in study_info["conditions"].items():
            if cond_name == control_cond:
                continue

            cond_path = download_bigwig(study_name, cond_name)
            if not cond_path:
                continue

            value = get_expression(cond_path, gene["chr"], gene["start"], gene["end"])
            if value is None:
                continue

            fold_change = value / control_value if control_value > 0 else 0

            study_results["conditions"].append({
                "name": cond_name,
                "label": cond_info["label"],
                "bucket": cond_info["bucket"],
                "value": round(value, 2),
                "fold_change": round(fold_change, 2),
            })

        # Sort by fold change
        study_results["conditions"].sort(key=lambda x: x["fold_change"], reverse=True)
        results["studies"][study_name] = study_results

    return results


def print_results(results: dict):
    """Print results in a readable format."""
    if "error" in results:
        print(f"Error: {results['error']}")
        return

    print(f"\n{'='*80}")
    print(f"EXPRESSION ANALYSIS: {results['gene']} ({results['description']})")
    print(f"Location: {results['location']}")
    print(f"{'='*80}")

    for study_name, study_data in results["studies"].items():
        print(f"\n## {study_name} - {study_data['category']}")
        print(f"   Control: {study_data['control']} (value: {study_data['control_value']})")
        print("-" * 70)
        print(f"   {'Condition':<30} {'Value':>10} {'Fold Change':>12} {'Category'}")
        print("-" * 70)

        for cond in study_data["conditions"]:
            fc = cond["fold_change"]
            # Add arrow indicator
            if fc >= 2:
                arrow = "↑↑"
            elif fc >= 1.5:
                arrow = "↑"
            elif fc <= 0.5:
                arrow = "↓↓"
            elif fc <= 0.67:
                arrow = "↓"
            else:
                arrow = "→"

            bucket = BUCKET_LABELS.get(cond["bucket"], cond["bucket"])
            print(f"   {cond['label']:<30} {cond['value']:>10.2f} {fc:>8.2f}x {arrow:<3} {bucket}")


def compare_genes(gene_names: list):
    """Compare fold changes across multiple genes."""
    print(f"\n{'='*80}")
    print("GENE COMPARISON - Fold Changes by Condition")
    print(f"{'='*80}")

    # Collect data
    all_data = {}
    for gene_name in gene_names:
        results = analyze_gene(gene_name)
        if "error" not in results:
            all_data[gene_name] = results

    # Print comparison table by study
    for study_name in STUDIES.keys():
        if not any(study_name in data["studies"] for data in all_data.values()):
            continue

        print(f"\n## {study_name}")

        # Get all conditions for this study
        first_gene = list(all_data.values())[0]
        if study_name not in first_gene["studies"]:
            continue

        conditions = first_gene["studies"][study_name]["conditions"]

        # Header
        print(f"   {'Condition':<25}", end="")
        for gene_name in gene_names:
            print(f"{gene_name:>10}", end="")
        print()
        print("-" * (30 + len(gene_names) * 10))

        # Data rows
        for cond in conditions:
            print(f"   {cond['label'][:25]:<25}", end="")
            for gene_name in gene_names:
                if study_name in all_data.get(gene_name, {}).get("studies", {}):
                    study_data = all_data[gene_name]["studies"][study_name]
                    gene_cond = next((c for c in study_data["conditions"] if c["name"] == cond["name"]), None)
                    if gene_cond:
                        fc = gene_cond["fold_change"]
                        print(f"{fc:>9.2f}x", end="")
                    else:
                        print(f"{'--':>10}", end="")
                else:
                    print(f"{'--':>10}", end="")
            print()


if __name__ == "__main__":
    print("Downloading and caching bigwig files (first run may take a moment)...")

    if len(sys.argv) > 1:
        if sys.argv[1] == "--compare":
            genes = sys.argv[2:] if len(sys.argv) > 2 else list(GENES.keys())
            compare_genes(genes)
        else:
            for gene in sys.argv[1:]:
                results = analyze_gene(gene)
                print_results(results)
    else:
        # Default: show HOG1 analysis
        results = analyze_gene("HOG1")
        print_results(results)

        # Also show comparison
        print("\n")
        compare_genes(["HOG1", "ALS1", "ACT1", "EFG1"])
