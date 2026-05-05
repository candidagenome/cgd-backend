#!/usr/bin/env python3
"""
Extract library sizes (total mapped reads) for expression normalization.

This script extracts the total number of mapped reads from BAM files
and outputs them in a format ready to paste into expression_service.py.

Usage:
    python extract_library_sizes.py [--method bam|bigwig] [--organism ORGANISM]

Methods:
    bam: Use samtools flagstat (more accurate, requires BAM files)
    bigwig: Use total BigWig signal as proxy (faster, less accurate)

Example:
    python extract_library_sizes.py --method bam --organism C_albicans_SC5314
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, Optional

# Try to import pyBigWig for bigwig method
try:
    import pyBigWig
    PYBIGWIG_AVAILABLE = True
except ImportError:
    PYBIGWIG_AVAILABLE = False

# Import configuration from expression_service
sys.path.insert(0, str(Path(__file__).parent.parent))
try:
    from cgd.api.services.expression_service import (
        EXPRESSION_STUDIES,
        HTS_BASE_PATHS,
        _get_bigwig_path,
    )
except ImportError:
    print("ERROR: Could not import expression_service. Run from project root.")
    sys.exit(1)


def get_bam_path_from_bigwig(bigwig_path: Path) -> Optional[Path]:
    """
    Infer BAM file path from BigWig path.

    BigWig paths typically look like:
      .../study/HapA/condition/sorted_hits_bam2wig/sorted_hits.bigwig
    BAM files are usually:
      .../study/HapA/condition/sorted_hits.bam
    """
    # Try common patterns
    parent = bigwig_path.parent

    # Pattern 1: sorted_hits_bam2wig/sorted_hits.bigwig -> ../sorted_hits.bam
    if parent.name == "sorted_hits_bam2wig":
        bam_path = parent.parent / "sorted_hits.bam"
        if bam_path.exists():
            return bam_path

    # Pattern 2: condition_bam2wig/condition.bigwig -> ../condition_sorted_hits.bam
    if "_bam2wig" in parent.name:
        condition = parent.name.replace("_bam2wig", "")
        bam_path = parent.parent / f"{condition}_sorted_hits.bam"
        if bam_path.exists():
            return bam_path

    # Pattern 3: condition/condition_sorted_hits.bigwig -> condition/condition_sorted_hits.bam
    bam_name = bigwig_path.stem + ".bam"
    bam_path = parent / bam_name
    if bam_path.exists():
        return bam_path

    # Pattern 4: Look for any .bam file in the same directory or parent
    for search_dir in [parent, parent.parent]:
        for bam_file in search_dir.glob("*.bam"):
            if "sorted" in bam_file.name:
                return bam_file

    return None


def get_mapped_reads_from_bam(bam_path: Path) -> Optional[int]:
    """Get total mapped reads from BAM using pysam or samtools."""
    # Try pysam first (preferred)
    try:
        import pysam
        bam = pysam.AlignmentFile(str(bam_path), "rb")
        # Get mapped reads from index stats
        stats = bam.get_index_statistics()
        total_mapped = sum(s.mapped for s in stats)
        bam.close()
        if total_mapped > 0:
            return total_mapped
    except ImportError:
        pass  # pysam not available, try samtools
    except Exception as e:
        print(f"  pysam error: {e}", file=sys.stderr)
        # Fall through to try samtools

    # Fall back to samtools command line
    try:
        result = subprocess.run(
            ["samtools", "flagstat", str(bam_path)],
            capture_output=True,
            text=True,
            timeout=300
        )
        if result.returncode != 0:
            return None

        # Parse flagstat output - look for "mapped (" line
        for line in result.stdout.split("\n"):
            if "mapped (" in line and "primary" not in line.lower():
                # Format: "12345678 + 0 mapped (95.00% : N/A)"
                count = int(line.split()[0])
                return count

        return None
    except Exception as e:
        print(f"  Error running samtools: {e}", file=sys.stderr)
        return None


def get_total_signal_from_bigwig(bigwig_path: Path) -> Optional[float]:
    """Get total signal from BigWig file (proxy for library size)."""
    if not PYBIGWIG_AVAILABLE:
        return None

    try:
        bw = pyBigWig.open(str(bigwig_path))
        if bw is None:
            return None

        total_signal = 0.0
        for chrom, length in bw.chroms().items():
            stats = bw.stats(chrom, 0, length, type="sum")
            if stats and stats[0] is not None:
                total_signal += stats[0]

        bw.close()
        return total_signal
    except Exception as e:
        print(f"  Error reading BigWig: {e}", file=sys.stderr)
        return None


def extract_library_sizes(
    organism: Optional[str] = None,
    method: str = "bam"
) -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    Extract library sizes for all configured studies.

    Returns:
        Dict structure matching LIBRARY_SIZES format:
        {organism: {study_id: {condition_id: library_size_in_millions}}}
    """
    library_sizes = {}

    organisms = [organism] if organism else list(EXPRESSION_STUDIES.keys())

    for org_key in organisms:
        if org_key not in EXPRESSION_STUDIES:
            print(f"WARNING: {org_key} not in EXPRESSION_STUDIES", file=sys.stderr)
            continue

        base_path = HTS_BASE_PATHS.get(org_key)
        if not base_path or not base_path.exists():
            print(f"WARNING: Base path not found for {org_key}", file=sys.stderr)
            continue

        library_sizes[org_key] = {}
        studies_config = EXPRESSION_STUDIES[org_key]

        for study_id, study_info in studies_config.items():
            print(f"\nProcessing {org_key}/{study_id}...")
            library_sizes[org_key][study_id] = {}

            for cond_id in study_info["conditions"].keys():
                bigwig_path = _get_bigwig_path(base_path, study_id, cond_id, study_info)

                if not bigwig_path.exists():
                    print(f"  {cond_id}: BigWig not found", file=sys.stderr)
                    continue

                if method == "bam":
                    bam_path = get_bam_path_from_bigwig(bigwig_path)
                    if bam_path and bam_path.exists():
                        mapped_reads = get_mapped_reads_from_bam(bam_path)
                        if mapped_reads:
                            # Convert to millions
                            lib_size_millions = round(mapped_reads / 1_000_000, 2)
                            library_sizes[org_key][study_id][cond_id] = lib_size_millions
                            print(f"  {cond_id}: {lib_size_millions}M reads")
                        else:
                            print(f"  {cond_id}: Could not parse BAM stats", file=sys.stderr)
                    else:
                        print(f"  {cond_id}: BAM file not found", file=sys.stderr)

                elif method == "bigwig":
                    total_signal = get_total_signal_from_bigwig(bigwig_path)
                    if total_signal:
                        # Use signal as proxy (arbitrary scale, not true read count)
                        lib_size_millions = round(total_signal / 1_000_000, 2)
                        library_sizes[org_key][study_id][cond_id] = lib_size_millions
                        print(f"  {cond_id}: {lib_size_millions}M signal")
                    else:
                        print(f"  {cond_id}: Could not read BigWig", file=sys.stderr)

    return library_sizes


def format_for_python(library_sizes: Dict) -> str:
    """Format library sizes as Python code for pasting into expression_service.py."""
    lines = ["LIBRARY_SIZES: Dict[str, Dict[str, Dict[str, float]]] = {"]

    for org_key, studies in library_sizes.items():
        lines.append(f'    "{org_key}": {{')

        for study_id, conditions in studies.items():
            lines.append(f'        "{study_id}": {{')

            for cond_id, lib_size in conditions.items():
                lines.append(f'            "{cond_id}": {lib_size},')

            lines.append("        },")

        lines.append("    },")

    lines.append("}")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Extract library sizes for expression normalization"
    )
    parser.add_argument(
        "--method",
        choices=["bam", "bigwig"],
        default="bam",
        help="Method to extract library size (default: bam)"
    )
    parser.add_argument(
        "--organism",
        help="Specific organism to process (default: all)"
    )
    parser.add_argument(
        "--output",
        choices=["python", "json"],
        default="python",
        help="Output format (default: python)"
    )
    parser.add_argument(
        "--output-file",
        help="Write output to file instead of stdout"
    )

    args = parser.parse_args()

    if args.method == "bigwig" and not PYBIGWIG_AVAILABLE:
        print("ERROR: pyBigWig not installed. Use --method bam or install pyBigWig.")
        sys.exit(1)

    if args.method == "bam":
        # Check if pysam or samtools is available
        pysam_available = False
        samtools_available = False
        try:
            import pysam
            pysam_available = True
        except ImportError:
            pass
        try:
            subprocess.run(["samtools", "--version"], capture_output=True, check=True)
            samtools_available = True
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass
        if not pysam_available and not samtools_available:
            print("ERROR: Neither pysam nor samtools found.")
            print("Install pysam (pip install pysam) or samtools, or use --method bigwig.")
            sys.exit(1)
        if pysam_available:
            print("Using pysam for BAM parsing")

    print("=" * 60)
    print(f"Extracting library sizes using {args.method} method")
    print("=" * 60)

    library_sizes = extract_library_sizes(args.organism, args.method)

    print("\n" + "=" * 60)
    print("Results")
    print("=" * 60)

    if args.output == "python":
        output = format_for_python(library_sizes)
    else:
        output = json.dumps(library_sizes, indent=2)

    if args.output_file:
        with open(args.output_file, "w") as f:
            f.write(output)
        print(f"Output written to {args.output_file}")
    else:
        print("\n" + output)

    print("\n" + "=" * 60)
    print("Next Steps")
    print("=" * 60)
    print("""
1. Copy the LIBRARY_SIZES output above

2. Paste it into cgd/api/services/expression_service.py,
   replacing the empty LIBRARY_SIZES dict

3. Set NORMALIZE_BY_LIBRARY_SIZE = True

4. Rebuild the expression cache:
   python scripts/build_expression_cache.py

5. Test the expression endpoint to verify fold changes are normalized
""")


if __name__ == "__main__":
    main()
