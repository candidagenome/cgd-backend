#!/usr/bin/env python3
"""
Convert EMBL files to FASTA format.

This script reads sequence files in EMBL format and converts them
to FASTA format.

Original Perl: emblToFasta.pl
Converted to Python: 2024
"""

import argparse
import sys
from pathlib import Path

from Bio import SeqIO


def convert_embl_to_fasta(
    input_files: list[Path],
    output_file: Path,
) -> int:
    """
    Convert EMBL files to FASTA format.

    Args:
        input_files: List of input EMBL files
        output_file: Output FASTA file

    Returns:
        Number of sequences converted
    """
    count = 0

    with open(output_file, 'w') as out_handle:
        for input_file in input_files:
            print(f"Processing: {input_file}", file=sys.stderr)

            for record in SeqIO.parse(str(input_file), 'embl'):
                SeqIO.write(record, out_handle, 'fasta')
                count += 1

    return count


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Convert EMBL files to FASTA format"
    )
    parser.add_argument(
        "input_files",
        type=Path,
        nargs="+",
        help="Input EMBL file(s)",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        required=True,
        help="Output FASTA file",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    # Validate input files
    for input_file in args.input_files:
        if not input_file.exists():
            print(f"Error: File not found: {input_file}", file=sys.stderr)
            sys.exit(1)

    try:
        count = convert_embl_to_fasta(args.input_files, args.output)
        if args.verbose:
            print(f"Converted {count} sequences to {args.output}")
    except Exception as e:
        print(f"Error converting files: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
