#!/usr/bin/env python3
"""
Expression Data Proof-of-Concept v3
- Handles both old and new bigwig file path patterns
- Includes more studies with proper SRR ID mappings
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
    "ERG11": {"chr": "Ca22chr5A_C_albicans_SC5314", "start": 233568, "end": 235196, "desc": "Azole target"},
    "HWP1": {"chr": "Ca22chrRA_C_albicans_SC5314", "start": 1617847, "end": 1619796, "desc": "Hyphal wall protein"},
}

# Studies configuration
# path_style: "old" = sorted_hits_bam2wig/sorted_hits.bigwig
#             "new" = {SRR_ID}_sorted_hits.bigwig
STUDIES = {
    "Bruno_2010": {
        "category": "Stress Response",
        "pmid": "20543895",
        "path_style": "old",
        "control": "nOxi",
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
        "path_style": "old",
        "control": "sc_plnk",
        "conditions": {
            "sc_plnk": {"label": "Planktonic", "bucket": "control"},
            "sc_film": {"label": "Biofilm", "bucket": "basic_biology"},
        },
    },
    "Niemiec_2017": {
        "category": "Immune Response",
        "pmid": "28874114",
        "path_style": "old",
        "control": "chg_77",
        "conditions": {
            "chg_77": {"label": "PMN Control 0-min", "bucket": "control"},
            "chg_71": {"label": "PMN 15-min", "bucket": "kill_candida"},
            "chg_72": {"label": "PMN 30-min", "bucket": "kill_candida"},
            "chg_73": {"label": "PMN 60-min", "bucket": "kill_candida"},
            "chg_75": {"label": "PMN Hyphae Control", "bucket": "control"},
            "chg_79": {"label": "PMN Hyphae 15-min", "bucket": "kill_candida"},
            "chg_76": {"label": "NET Control", "bucket": "control"},
            "chg_70": {"label": "NETs Yeast", "bucket": "kill_candida"},
            "chg_69": {"label": "NETs Hyphae", "bucket": "kill_candida"},
        },
    },
    "Xie_2013": {
        "category": "Cell Type Switching",
        "pmid": "23326225",
        "path_style": "old",
        "control": "CY110wh",
        "conditions": {
            "CY110wh": {"label": "White cells", "bucket": "control"},
            "CY110op": {"label": "Opaque cells", "bucket": "basic_biology"},
        },
    },
    "Shivarathri_2019": {
        "category": "Antifungal Response",
        "pmid": "31263212",
        "path_style": "new",
        "control": "SRR8285058",  # WT control rep I
        "conditions": {
            "SRR8285058": {"label": "WT Control (rep I)", "bucket": "control"},
            "SRR8285059": {"label": "Caspofungin 15min (rep I)", "bucket": "kill_candida"},
            "SRR8285060": {"label": "Caspofungin 45min (rep I)", "bucket": "kill_candida"},
            "SRR8285064": {"label": "WT Control (rep II)", "bucket": "control"},
            "SRR8285065": {"label": "Caspofungin 15min (rep II)", "bucket": "kill_candida"},
            "SRR8285066": {"label": "Caspofungin 45min (rep II)", "bucket": "kill_candida"},
        },
    },
    "Glazier_2023": {
        "category": "Morphology/Media",
        "pmid": "37737633",
        "path_style": "new",
        "control": "SRR25396044",  # WT YPD control
        "conditions": {
            "SRR25396044": {"label": "YPD Control (rep A)", "bucket": "control"},
            "SRR25396043": {"label": "YPD Control (rep B)", "bucket": "control"},
            "SRR25396042": {"label": "RPMI 37°C (rep A)", "bucket": "basic_biology"},
            "SRR25396041": {"label": "RPMI 37°C (rep B)", "bucket": "basic_biology"},
            "SRR25396040": {"label": "Spider 37°C (rep A)", "bucket": "basic_biology"},
            "SRR25396039": {"label": "Spider 37°C (rep B)", "bucket": "basic_biology"},
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


def get_bigwig_url(study: str, condition: str, study_info: dict, haplotype: str = "HapA") -> str:
    """Construct URL to bigwig file based on path style."""
    if study_info["path_style"] == "old":
        return f"{HTS_BASE_URL}/{study}/{haplotype}/{condition}/sorted_hits_bam2wig/sorted_hits.bigwig"
    else:  # new style
        return f"{HTS_BASE_URL}/{study}/{haplotype}/{condition}/{condition}_sorted_hits.bigwig"


def download_bigwig(url: str) -> Path:
    """Download bigwig file if not cached."""
    cache_path = get_cache_path(url)

    if not cache_path.exists():
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        try:
            print(f"  Downloading: {url.split('/')[-3]}/{url.split('/')[-1][:20]}...", end=" ", flush=True)
            urllib.request.urlretrieve(url, cache_path)
            print("OK")
        except Exception as e:
            print(f"FAILED: {e}")
            return None

    return cache_path


def get_expression(bigwig_path: Path, chrom: str, start: int, end: int) -> float:
    """Get mean expression value for a region."""
    try:
        bw = pyBigWig.open(str(bigwig_path))
        stats = bw.stats(chrom, start - 1, end, type="mean")
        bw.close()
        return stats[0] if stats and stats[0] else 0.0
    except Exception as e:
        return None


def analyze_gene(gene_name: str, verbose: bool = True) -> dict:
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
        control_url = get_bigwig_url(study_name, control_cond, study_info)
        control_path = download_bigwig(control_url)

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

            cond_url = get_bigwig_url(study_name, cond_name, study_info)
            cond_path = download_bigwig(cond_url)
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
        print(f"   Control: {study_data['control'][:20]} (value: {study_data['control_value']})")
        print("-" * 70)
        print(f"   {'Condition':<35} {'Value':>8} {'FC':>8} {'Category'}")
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

            bucket = BUCKET_LABELS.get(cond["bucket"], cond["bucket"])[:15]
            print(f"   {cond['label'][:35]:<35} {cond['value']:>8.1f} {fc:>6.2f}x{arrow:<2} {bucket}")


def compare_genes(gene_names: list):
    """Compare fold changes across multiple genes."""
    print(f"\n{'='*80}")
    print("GENE COMPARISON - Fold Changes by Condition")
    print(f"{'='*80}")

    # Collect data
    all_data = {}
    for gene_name in gene_names:
        print(f"\nProcessing {gene_name}...")
        results = analyze_gene(gene_name, verbose=False)
        if "error" not in results:
            all_data[gene_name] = results

    # Print comparison table by study
    for study_name, study_info in STUDIES.items():
        if not any(study_name in data["studies"] for data in all_data.values()):
            continue

        first_gene_with_study = None
        for gn, data in all_data.items():
            if study_name in data["studies"]:
                first_gene_with_study = gn
                break

        if not first_gene_with_study:
            continue

        study_data = all_data[first_gene_with_study]["studies"][study_name]
        print(f"\n## {study_name} - {study_data['category']}")

        # Header
        print(f"   {'Condition':<30}", end="")
        for gene_name in gene_names:
            print(f"{gene_name:>10}", end="")
        print()
        print("-" * (35 + len(gene_names) * 10))

        # Data rows
        for cond in study_data["conditions"]:
            print(f"   {cond['label'][:30]:<30}", end="")
            for gene_name in gene_names:
                if gene_name in all_data and study_name in all_data[gene_name]["studies"]:
                    gene_study = all_data[gene_name]["studies"][study_name]
                    gene_cond = next((c for c in gene_study["conditions"] if c["name"] == cond["name"]), None)
                    if gene_cond:
                        fc = gene_cond["fold_change"]
                        if fc >= 2:
                            print(f"{fc:>8.2f}x↑", end="")
                        elif fc <= 0.5:
                            print(f"{fc:>8.2f}x↓", end="")
                        else:
                            print(f"{fc:>9.2f}x", end="")
                    else:
                        print(f"{'--':>10}", end="")
                else:
                    print(f"{'--':>10}", end="")
            print()


def summary_by_bucket(gene_names: list):
    """Summarize expression changes by bucket category."""
    print(f"\n{'='*80}")
    print("SUMMARY BY CATEGORY")
    print(f"{'='*80}")

    # Collect all data
    all_data = {}
    for gene_name in gene_names:
        results = analyze_gene(gene_name, verbose=False)
        if "error" not in results:
            all_data[gene_name] = results

    # Organize by bucket
    bucket_data = {b: [] for b in BUCKET_LABELS.keys() if b != "control"}

    for gene_name, gene_data in all_data.items():
        for study_name, study_data in gene_data["studies"].items():
            for cond in study_data["conditions"]:
                bucket = cond["bucket"]
                if bucket != "control":
                    bucket_data[bucket].append({
                        "gene": gene_name,
                        "study": study_name,
                        "condition": cond["label"],
                        "fold_change": cond["fold_change"],
                    })

    # Print summary
    for bucket, label in BUCKET_LABELS.items():
        if bucket == "control":
            continue

        entries = bucket_data.get(bucket, [])
        if not entries:
            continue

        print(f"\n## {label}")
        print("-" * 70)

        # Sort by absolute fold change
        entries.sort(key=lambda x: abs(x["fold_change"] - 1), reverse=True)

        for e in entries[:10]:  # Top 10
            fc = e["fold_change"]
            direction = "↑" if fc > 1.2 else "↓" if fc < 0.8 else "→"
            print(f"   {e['gene']:<8} {fc:>6.2f}x {direction}  {e['condition'][:30]:<30} [{e['study']}]")


if __name__ == "__main__":
    print("Expression Analysis Tool - Proof of Concept v3")
    print("=" * 50)

    if len(sys.argv) > 1:
        if sys.argv[1] == "--compare":
            genes = sys.argv[2:] if len(sys.argv) > 2 else list(GENES.keys())[:5]
            compare_genes(genes)
        elif sys.argv[1] == "--summary":
            genes = sys.argv[2:] if len(sys.argv) > 2 else list(GENES.keys())[:5]
            summary_by_bucket(genes)
        else:
            for gene in sys.argv[1:]:
                results = analyze_gene(gene)
                print_results(results)
    else:
        # Default: show CDR1 analysis (drug efflux pump - relevant for antifungal response)
        print("\nAnalyzing CDR1 (Drug efflux pump) - relevant for antifungal response")
        results = analyze_gene("CDR1")
        print_results(results)

        print("\n" + "=" * 80)
        print("Running gene comparison...")
        compare_genes(["CDR1", "ERG11", "HOG1", "ACT1"])
