#!/usr/bin/env python3
"""
Parse ORFs with internal stops close to terminus.

This script filters sequences with internal stop codons to identify those
where the stop is close to the C-terminus (within 10 residues), which may
indicate annotation errors or frameshifts rather than true pseudogenes.

Original Perl: parseInternalStopsCloseToTerminus.pl
Converted to Python: 2024
"""

import argparse
import re
import sys
from pathlib import Path

from Bio import SeqIO
from Bio.SeqRecord import SeqRecord


def filter_stops_near_terminus(
    input_file: Path,
    distance: int = 10,
) -> tuple[list[str], dict[str, SeqRecord]]:
    """
    Filter sequences with internal stops close to terminus.

    Args:
        input_file: Input FASTA file with sequences
        distance: Distance from terminus to check (default: 10 residues)

    Returns:
        Tuple of (list of ORF names with near-terminus stops, dict of all sequences)
    """
    near_terminus_orfs = set()
    all_sequences = {}

    for record in SeqIO.parse(str(input_file), 'fasta'):
        # Store the sequence
        all_sequences[record.id] = record

        # Only check translated sequences (not _NA suffix)
        if '_NA' in record.id:
            continue

        seq = str(record.seq)
        seq_length = len(seq)

        if seq_length <= distance:
            continue

        # Check last N residues for stop codons
        c_terminus = seq[seq_length - distance:seq_length - 1]

        if '*' in c_terminus:
            # Extract ORF name
            match = re.search(r'(orf\d+\.\d+\.?\d*)', record.id, re.IGNORECASE)
            if match:
                orf_name = match.group(1)
                near_terminus_orfs.add(orf_name)
                print(record.id, file=sys.stderr)

    return list(near_terminus_orfs), all_sequences


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Parse ORFs with internal stops close to terminus"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input FASTA file with sequences (both NA and translated)",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Output FASTA file (default: stdout)",
    )
    parser.add_argument(
        "--distance",
        type=int,
        default=10,
        help="Distance from terminus to check (default: 10 residues)",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Only output list of ORF names, not sequences",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    # Validate input
    if not args.input_file.exists():
        print(f"Error: File not found: {args.input_file}", file=sys.stderr)
        sys.exit(1)

    # Filter sequences
    near_terminus_orfs, all_sequences = filter_stops_near_terminus(
        args.input_file,
        args.distance,
    )

    if args.verbose:
        print(
            f"Found {len(near_terminus_orfs)} ORFs with stops within "
            f"{args.distance} residues of terminus",
            file=sys.stderr,
        )

    # Output
    out_handle = open(args.output, 'w') if args.output else sys.stdout

    try:
        if args.list_only:
            for orf_name in sorted(near_terminus_orfs):
                out_handle.write(f"{orf_name}\n")
        else:
            # Output all sequences for matching ORFs
            for seq_id, record in all_sequences.items():
                match = re.search(r'(orf\d+\.\d+\.?\d*)', seq_id, re.IGNORECASE)
                if match:
                    orf_name = match.group(1)
                    if orf_name in near_terminus_orfs:
                        SeqIO.write(record, out_handle, 'fasta')

    finally:
        if args.output:
            out_handle.close()


if __name__ == "__main__":
    main()
