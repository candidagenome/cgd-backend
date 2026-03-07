#!/usr/bin/env python3
"""
Generate byte offsets for FASTA sequence headers.

This script generates a list of byte offsets where each sequence begins
in a FASTA file. Output format: byte_offset<TAB>sequence_id

Original Perl: generate_sequence_index.pl
Converted to Python: 2024

Usage:
    python generate_sequence_index.py sequences.fasta > index.txt
    cat sequences.fasta | python generate_sequence_index.py > index.txt
"""

import argparse
import sys
from pathlib import Path


def generate_index(input_file=None) -> None:
    """
    Generate byte offset index for FASTA file.

    Args:
        input_file: Input FASTA file path, or None for stdin
    """
    if input_file:
        fh = open(input_file, 'r')
    else:
        fh = sys.stdin

    bytecount = 0

    try:
        for line in fh:
            if line.startswith('>'):
                # Extract sequence ID (first word after >)
                seq_id = line[1:].split()[0] if line[1:].split() else ''
                # Print byte offset after the header line + sequence ID
                print(f"{bytecount + len(line)}\t{seq_id}")

            bytecount += len(line.encode('utf-8'))
    finally:
        if input_file:
            fh.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate byte offsets for FASTA sequence headers"
    )
    parser.add_argument(
        "input_file",
        nargs='?',
        type=Path,
        help="Input FASTA file (default: stdin)",
    )

    args = parser.parse_args()

    if args.input_file and not args.input_file.exists():
        print(f"Error: File not found: {args.input_file}", file=sys.stderr)
        sys.exit(1)

    generate_index(args.input_file)


if __name__ == "__main__":
    main()
