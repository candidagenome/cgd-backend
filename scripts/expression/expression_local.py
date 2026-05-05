#!/usr/bin/env python3
"""
Expression Analysis - Local NFS version
Reads bigwig files directly from NFS mount
"""
import os
import sys
from pathlib import Path

import pyBigWig

HTS_BASE = Path("/data/HTS/C_albicans_SC5314/bam")

GENES = {
    "HOG1": {"chr": "Ca22chr1A_C_albicans_SC5314", "start": 1533366, "end": 1534691, "desc": "Stress response kinase"},
    "ALS1": {"chr": "Ca22chr6A_C_albicans_SC5314", "start": 32667, "end": 36569, "desc": "Adhesin/virulence"},
    "ACT1": {"chr": "Ca22chr3A_C_albicans_SC5314", "start": 449217, "end": 450494, "desc": "Housekeeping"},
    "EFG1": {"chr": "Ca22chrRA_C_albicans_SC5314", "start": 753953, "end": 755713, "desc": "Morphology regulator"},
    "CDR1": {"chr": "Ca22chr3A_C_albicans_SC5314", "start": 197636, "end": 202135, "desc": "Drug efflux pump"},
    "ERG11": {"chr": "Ca22chr5A_C_albicans_SC5314", "start": 233568, "end": 235196, "desc": "Azole target"},
    "HWP1": {"chr": "Ca22chrRA_C_albicans_SC5314", "start": 1617847, "end": 1619796, "desc": "Hyphal wall protein"},
}

STUDIES = {
    "Bruno_2010": {
        "category": "Stress Response",
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
        "path_style": "old",
        "control": "sc_plnk",
        "conditions": {
            "sc_plnk": {"label": "Planktonic", "bucket": "control"},
            "sc_film": {"label": "Biofilm", "bucket": "basic_biology"},
        },
    },
    "Niemiec_2017": {
        "category": "Immune Response",
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
        "category": "Cell Type",
        "path_style": "old",
        "control": "CY110wh",
        "conditions": {
            "CY110wh": {"label": "White cells", "bucket": "control"},
            "CY110op": {"label": "Opaque cells", "bucket": "basic_biology"},
        },
    },
    "Shivarathri_2019": {
        "category": "Antifungal (Caspofungin)",
        "path_style": "new",
        "control": "SRR8285058",
        "conditions": {
            "SRR8285058": {"label": "WT Control (rep I)", "bucket": "control"},
            "SRR8285059": {"label": "Caspofungin 15min (I)", "bucket": "kill_candida"},
            "SRR8285060": {"label": "Caspofungin 45min (I)", "bucket": "kill_candida"},
            "SRR8285064": {"label": "WT Control (rep II)", "bucket": "control"},
            "SRR8285065": {"label": "Caspofungin 15min (II)", "bucket": "kill_candida"},
            "SRR8285066": {"label": "Caspofungin 45min (II)", "bucket": "kill_candida"},
        },
    },
    "Glazier_2023": {
        "category": "Morphology/Media",
        "path_style": "new",
        "control": "SRR25396044",
        "conditions": {
            "SRR25396044": {"label": "YPD Control (A)", "bucket": "control"},
            "SRR25396043": {"label": "YPD Control (B)", "bucket": "control"},
            "SRR25396042": {"label": "RPMI 37C (A)", "bucket": "basic_biology"},
            "SRR25396041": {"label": "RPMI 37C (B)", "bucket": "basic_biology"},
            "SRR25396040": {"label": "Spider 37C (A)", "bucket": "basic_biology"},
            "SRR25396039": {"label": "Spider 37C (B)", "bucket": "basic_biology"},
        },
    },
}

def get_bigwig_path(study, condition, study_info, hap="HapA"):
    if study_info["path_style"] == "old":
        return HTS_BASE / study / hap / condition / "sorted_hits_bam2wig" / "sorted_hits.bigwig"
    else:
        return HTS_BASE / study / hap / condition / f"{condition}_sorted_hits.bigwig"

def get_expression(path, chrom, start, end):
    try:
        bw = pyBigWig.open(str(path))
        stats = bw.stats(chrom, start - 1, end, type="mean")
        bw.close()
        return stats[0] if stats and stats[0] else 0.0
    except:
        return None

def analyze_all_genes():
    print("\n" + "="*100)
    print("FULL EXPRESSION ANALYSIS - Fold Changes (Local NFS)")
    print("="*100)

    for study_name, study_info in STUDIES.items():
        print(f"\n## {study_name} - {study_info['category']}")

        # Header
        print(f"{'Condition':<30}", end="")
        for gene in GENES:
            print(f"{gene:>10}", end="")
        print()
        print("-" * (30 + len(GENES) * 10))

        control = study_info["control"]
        control_path = get_bigwig_path(study_name, control, study_info)

        if not control_path.exists():
            print(f"   Control file not found: {control_path}")
            continue

        # Get control values for all genes
        control_values = {}
        for gene, info in GENES.items():
            val = get_expression(control_path, info["chr"], info["start"], info["end"])
            control_values[gene] = val if val and val > 0 else None

        # Process each condition
        for cond, cond_info in study_info["conditions"].items():
            if cond == control:
                continue

            cond_path = get_bigwig_path(study_name, cond, study_info)
            if not cond_path.exists():
                continue

            print(f"{cond_info['label'][:30]:<30}", end="")

            for gene, info in GENES.items():
                val = get_expression(cond_path, info["chr"], info["start"], info["end"])
                ctrl = control_values.get(gene)

                if val is not None and ctrl:
                    fc = val / ctrl
                    if fc >= 2:
                        print(f"{fc:>8.2f}x↑", end="")
                    elif fc <= 0.5:
                        print(f"{fc:>8.2f}x↓", end="")
                    else:
                        print(f"{fc:>9.2f}x", end="")
                else:
                    print(f"{'--':>10}", end="")
            print()

if __name__ == "__main__":
    analyze_all_genes()
