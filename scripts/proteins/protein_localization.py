#!/usr/bin/env python3
"""
Run protein localization predictions.

This script runs SignalP and WoLF PSORT to predict protein localization,
including signal peptides and subcellular localization.

Original Perl: ProteinLocalization.pl
Converted to Python: 2024
"""

import argparse
import gzip
import subprocess
import sys
from pathlib import Path


def run_signalp(
    protein_file: Path,
    signalp_path: str = 'signalp',
    organism_type: str = 'euk',
) -> dict[str, dict]:
    """
    Run SignalP prediction.

    Args:
        protein_file: Input FASTA file
        signalp_path: Path to SignalP executable
        organism_type: Organism type (euk, gram+, gram-)

    Returns:
        Dict mapping protein ID to SignalP results
    """
    results = {}

    # Determine input command
    if str(protein_file).endswith('.gz'):
        cat_cmd = ['gzcat', str(protein_file)]
    else:
        cat_cmd = ['cat', str(protein_file)]

    # Run SignalP
    print(f"Running SignalP...", file=sys.stderr)

    cat_proc = subprocess.Popen(cat_cmd, stdout=subprocess.PIPE)
    signalp_cmd = [signalp_path, '-t', organism_type, '-f', 'short']

    proc = subprocess.Popen(
        signalp_cmd,
        stdin=cat_proc.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    cat_proc.stdout.close()

    stdout, stderr = proc.communicate()

    if proc.returncode != 0:
        print(f"SignalP error: {stderr}", file=sys.stderr)

    # Parse output
    for line in stdout.split('\n'):
        line = line.strip()
        if not line or line.startswith('#'):
            continue

        parts = line.split()
        if len(parts) >= 10:
            protein_id = parts[0]
            d_prob = float(parts[8])  # D-score
            d_yn = parts[9]  # Y/N prediction

            results[protein_id] = {
                'd_prob': d_prob,
                'd_yn': d_yn,
            }

    return results


def run_wolfpsort(
    protein_file: Path,
    wolfpsort_path: str = 'runWolfPsortSummary',
    organism_type: str = 'fungi',
) -> dict[str, dict]:
    """
    Run WoLF PSORT prediction.

    Args:
        protein_file: Input FASTA file
        wolfpsort_path: Path to WoLF PSORT executable
        organism_type: Organism type (fungi, animal, plant)

    Returns:
        Dict mapping protein ID to WoLF PSORT results
    """
    results = {}

    # Determine input command
    if str(protein_file).endswith('.gz'):
        cat_cmd = ['gzcat', str(protein_file)]
    else:
        cat_cmd = ['cat', str(protein_file)]

    # Run WoLF PSORT
    print(f"Running WoLF PSORT...", file=sys.stderr)

    cat_proc = subprocess.Popen(cat_cmd, stdout=subprocess.PIPE)
    wps_cmd = [wolfpsort_path, organism_type]

    proc = subprocess.Popen(
        wps_cmd,
        stdin=cat_proc.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    cat_proc.stdout.close()

    stdout, stderr = proc.communicate()

    if proc.returncode != 0:
        print(f"WoLF PSORT error: {stderr}", file=sys.stderr)

    # Parse output
    for line in stdout.split('\n'):
        line = line.strip()
        if not line or line.startswith('#'):
            continue

        # Format: protein_id location1 score1, location2 score2, ...
        if ' ' in line or '\t' in line:
            parts = line.split(None, 1)
            protein_id = parts[0]
            locations = parts[1] if len(parts) > 1 else ''

            # Parse locations
            loc_list = []
            extr_score = 0

            for loc in locations.split(','):
                loc = loc.strip()
                if ' ' in loc:
                    loc_parts = loc.split()
                    loc_type = loc_parts[0]
                    try:
                        score = float(loc_parts[1])
                    except (IndexError, ValueError):
                        score = 0

                    loc_list.append((loc_type, score))

                    if loc_type == 'extr' and score > extr_score:
                        extr_score = score

            results[protein_id] = {
                'locations': locations,
                'loc_list': loc_list,
                'extr': 'extr' if extr_score > 0 else '-',
                'extr_score': extr_score if extr_score > 0 else '-',
            }

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run protein localization predictions (SignalP + WoLF PSORT)"
    )
    parser.add_argument(
        "protein_file",
        type=Path,
        help="Input protein FASTA file",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Output file (default: stdout)",
    )
    parser.add_argument(
        "--signalp-path",
        default="signalp",
        help="Path to SignalP executable (default: signalp)",
    )
    parser.add_argument(
        "--wolfpsort-path",
        default="runWolfPsortSummary",
        help="Path to WoLF PSORT executable",
    )
    parser.add_argument(
        "--organism-type",
        choices=['euk', 'gram+', 'gram-'],
        default='euk',
        help="Organism type for SignalP (default: euk)",
    )
    parser.add_argument(
        "--wolfpsort-type",
        choices=['fungi', 'animal', 'plant'],
        default='fungi',
        help="Organism type for WoLF PSORT (default: fungi)",
    )
    parser.add_argument(
        "--signalp-only",
        action="store_true",
        help="Only run SignalP",
    )
    parser.add_argument(
        "--wolfpsort-only",
        action="store_true",
        help="Only run WoLF PSORT",
    )

    args = parser.parse_args()

    # Validate input
    if not args.protein_file.exists():
        print(f"Error: File not found: {args.protein_file}", file=sys.stderr)
        sys.exit(1)

    # Run predictions
    signalp_results = {}
    wolfpsort_results = {}

    if not args.wolfpsort_only:
        signalp_results = run_signalp(
            args.protein_file,
            args.signalp_path,
            args.organism_type,
        )

    if not args.signalp_only:
        wolfpsort_results = run_wolfpsort(
            args.protein_file,
            args.wolfpsort_path,
            args.wolfpsort_type,
        )

    # Combine results
    all_ids = set(signalp_results.keys()) | set(wolfpsort_results.keys())

    # Output
    out_handle = open(args.output, 'w') if args.output else sys.stdout

    try:
        # Header
        out_handle.write(
            "ID\tD_Prob\tD_Y_N\tWoLF_PSORT_complete\tWoLF_PSORT_extr\tWoLF_PSORT_extr_score\n"
        )

        for protein_id in sorted(all_ids):
            sp = signalp_results.get(protein_id, {})
            wps = wolfpsort_results.get(protein_id, {})

            d_prob = sp.get('d_prob', '-')
            d_yn = sp.get('d_yn', '-')
            locations = wps.get('locations', '-')
            extr = wps.get('extr', '-')
            extr_score = wps.get('extr_score', '-')

            if isinstance(d_prob, float):
                d_prob = f"{d_prob:.3f}"

            out_handle.write(
                f"{protein_id}\t{d_prob}\t{d_yn}\t{locations}\t{extr}\t{extr_score}\n"
            )

    finally:
        if args.output:
            out_handle.close()


if __name__ == "__main__":
    main()
