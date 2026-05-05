#!/usr/bin/env python3
"""
Check if BigWig files are normalized or raw coverage.

Signs of NORMALIZED files (CPM/RPKM):
- Values typically in range 0-100 (or 0-1000 for highly expressed genes)
- Sum across genome ≈ 1 million (for CPM)
- Similar total signal across samples from the same study

Signs of RAW coverage:
- Values in thousands or millions
- Total signal varies dramatically between samples (proportional to sequencing depth)

Usage:
    python check_bigwig_normalization.py
"""
import sys
from pathlib import Path

try:
    import pyBigWig
except ImportError:
    print("ERROR: pyBigWig not installed. Run: pip install pyBigWig")
    sys.exit(1)

# Configuration - adjust paths as needed
HTS_BASE = Path("/data/HTS")

# Sample files to check from different studies
SAMPLE_FILES = [
    # C. albicans - Bruno 2010 (old style)
    ("C_albicans_SC5314/bam/Bruno_2010/HapA/nOxi/sorted_hits_bam2wig/sorted_hits.bigwig", "Bruno_2010 control"),
    ("C_albicans_SC5314/bam/Bruno_2010/HapA/hOxi/sorted_hits_bam2wig/sorted_hits.bigwig", "Bruno_2010 high H2O2"),
    # C. albicans - Shivarathri 2019 (new style)
    ("C_albicans_SC5314/bam/Shivarathri_2019/HapA/SRR8285058/SRR8285058_sorted_hits.bigwig", "Shivarathri_2019 control"),
    ("C_albicans_SC5314/bam/Shivarathri_2019/HapA/SRR8285059/SRR8285059_sorted_hits.bigwig", "Shivarathri_2019 CSP 15min"),
    # C. auris (if available)
    ("C_auris_B8441/bam/Shivarathri_2021/SRR17242148/SRR17242148_sorted_hits.bigwig", "C.auris Shivarathri control"),
]

# Well-known housekeeping genes to check (should have similar expression if normalized)
# ACT1 coordinates on Ca22 chr1
HOUSEKEEPING_GENES = {
    "ACT1": ("Ca22chr1A_C_albicans_SC5314", 1700000, 1702000),  # Approximate
    "TDH3": ("Ca22chr5A_C_albicans_SC5314", 400000, 402000),    # Approximate
}


def check_bigwig_stats(filepath: Path, label: str) -> dict:
    """Get statistics from a BigWig file."""
    if not filepath.exists():
        return {"error": f"File not found: {filepath}"}

    try:
        bw = pyBigWig.open(str(filepath))
        if bw is None:
            return {"error": "Could not open BigWig file"}

        # Get chromosome info
        chroms = bw.chroms()
        total_bases = sum(chroms.values())

        # Sample some values across the genome
        sample_values = []
        max_value = 0

        for chrom, length in list(chroms.items())[:5]:  # Check first 5 chromosomes
            # Get stats for the whole chromosome
            stats = bw.stats(chrom, 0, length, type="mean")
            if stats and stats[0] is not None:
                sample_values.append(stats[0])

            # Get max value
            max_stats = bw.stats(chrom, 0, length, type="max")
            if max_stats and max_stats[0] is not None:
                max_value = max(max_value, max_stats[0])

        # Calculate approximate total signal (sum across genome)
        # This is an approximation - true CPM would sum to ~1 million
        total_signal = 0
        for chrom, length in chroms.items():
            stats = bw.stats(chrom, 0, length, type="sum")
            if stats and stats[0] is not None:
                total_signal += stats[0]

        bw.close()

        avg_coverage = sum(sample_values) / len(sample_values) if sample_values else 0

        return {
            "label": label,
            "file": str(filepath.name),
            "chromosomes": len(chroms),
            "total_bases": total_bases,
            "avg_coverage": round(avg_coverage, 4),
            "max_value": round(max_value, 2),
            "total_signal": round(total_signal, 0),
        }
    except Exception as e:
        return {"error": str(e)}


def check_gene_values(filepath: Path, gene_name: str, chrom: str, start: int, end: int) -> float:
    """Get expression value for a specific gene region."""
    if not filepath.exists():
        return None

    try:
        bw = pyBigWig.open(str(filepath))
        if bw is None:
            return None

        # Check if chromosome exists
        chroms = bw.chroms()
        if chrom not in chroms:
            # Try without the suffix
            for c in chroms:
                if c.startswith(chrom.split("_")[0]):
                    chrom = c
                    break

        stats = bw.stats(chrom, start, end, type="mean")
        bw.close()

        if stats and stats[0] is not None:
            return round(stats[0], 4)
        return 0.0
    except Exception as e:
        return None


def main():
    print("=" * 70)
    print("BigWig Normalization Check")
    print("=" * 70)

    print("\n## File Statistics\n")
    print(f"{'Label':<30} {'Avg Cov':>12} {'Max Value':>12} {'Total Signal':>15}")
    print("-" * 70)

    results = []
    for rel_path, label in SAMPLE_FILES:
        filepath = HTS_BASE / rel_path
        stats = check_bigwig_stats(filepath, label)
        results.append(stats)

        if "error" in stats:
            print(f"{label:<30} ERROR: {stats['error']}")
        else:
            print(f"{stats['label']:<30} {stats['avg_coverage']:>12.4f} {stats['max_value']:>12.2f} {stats['total_signal']:>15,.0f}")

    print("\n" + "=" * 70)
    print("## Interpretation Guide")
    print("=" * 70)
    print("""
If NORMALIZED (CPM/RPKM/BPM):
  - Avg coverage: typically 0.1 - 10
  - Max values: typically < 1000
  - Total signal: ~1,000,000 (for CPM) or varies (for RPKM/BPM)
  - Similar total signal across samples from same study

If RAW COVERAGE:
  - Avg coverage: can be 10 - 1000+
  - Max values: can be 10,000 - 100,000+
  - Total signal: varies dramatically (proportional to read depth)
  - 2x deeper sequencing = 2x total signal
""")

    # Check if total signals vary significantly (sign of raw data)
    valid_signals = [r.get("total_signal", 0) for r in results if "error" not in r and r.get("total_signal", 0) > 0]
    if len(valid_signals) >= 2:
        min_sig = min(valid_signals)
        max_sig = max(valid_signals)
        ratio = max_sig / min_sig if min_sig > 0 else 0

        print(f"\n## Signal Variation Analysis")
        print(f"Min total signal: {min_sig:,.0f}")
        print(f"Max total signal: {max_sig:,.0f}")
        print(f"Ratio (max/min): {ratio:.2f}x")

        if ratio > 2:
            print("\n>>> WARNING: Large variation in total signal suggests RAW (unnormalized) data!")
            print(">>> Fold changes may be biased by sequencing depth differences.")
        else:
            print("\n>>> Signal variation is low - data may be normalized.")

    print("\n" + "=" * 70)
    print("## Recommendation")
    print("=" * 70)
    print("""
To definitively check normalization:

1. Check how BigWig files were created:
   - Look for bamCoverage or similar commands in processing scripts
   - Check for --normalizeUsing CPM/RPKM/BPM flags

2. Check BAM file headers for read counts:
   samtools flagstat <sample.bam>

3. If raw coverage, options are:
   a) Re-generate BigWig files with normalization:
      bamCoverage -b sample.bam -o sample.bw --normalizeUsing CPM

   b) Add library sizes to config and normalize at query time
""")


if __name__ == "__main__":
    main()
