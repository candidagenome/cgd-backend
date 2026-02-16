#!/usr/bin/env python3
"""
Run MUSCLE alignment on sequence groups.

This script runs MUSCLE multiple sequence alignment on groups of sequences,
useful for comparing spliced vs unspliced versions of intron-containing genes.

Original Perl: runMuscle.pl
Converted to Python: 2024
"""

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path


def parse_sequence_groups(input_file: Path) -> list[list[tuple[str, str]]]:
    """
    Parse FASTA file into groups of sequences.

    Groups are delimited by sequences ending with '_spliced' in their ID.

    Args:
        input_file: Input FASTA file

    Returns:
        List of sequence groups, each group is a list of (header, sequence) tuples
    """
    groups = []
    current_group = []
    current_header = None
    current_seq = []

    with open(input_file) as f:
        for line in f:
            line = line.rstrip()
            if not line:
                continue

            if line.startswith('>'):
                # Save previous sequence
                if current_header:
                    current_group.append((current_header, ''.join(current_seq)))

                    # Check if this completes a group
                    if '_spliced' in current_header.lower():
                        groups.append(current_group)
                        current_group = []

                current_header = line[1:]
                current_seq = []
            else:
                current_seq.append(line)

        # Don't forget the last sequence
        if current_header:
            current_group.append((current_header, ''.join(current_seq)))
            if '_spliced' in current_header.lower():
                groups.append(current_group)

    return groups


def run_muscle(
    sequences: list[tuple[str, str]],
    muscle_path: str = 'muscle',
    max_iters: int = 16,
    output_format: str = 'clw',
) -> str:
    """
    Run MUSCLE on a group of sequences.

    Args:
        sequences: List of (header, sequence) tuples
        muscle_path: Path to MUSCLE executable
        max_iters: Maximum iterations
        output_format: Output format (fasta, clw)

    Returns:
        Alignment output string
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.fasta', delete=False) as tmp_in:
        for header, seq in sequences:
            tmp_in.write(f">{header}\n{seq}\n")
        tmp_in_path = tmp_in.name

    with tempfile.NamedTemporaryFile(suffix='.aln', delete=False) as tmp_out:
        tmp_out_path = tmp_out.name

    try:
        cmd = [
            muscle_path,
            '-in', tmp_in_path,
            '-out', tmp_out_path,
            '-maxiters', str(max_iters),
        ]

        if output_format == 'clw':
            cmd.append('-clw')

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            print(f"MUSCLE error: {result.stderr}", file=sys.stderr)
            return ""

        with open(tmp_out_path) as f:
            return f.read()

    finally:
        Path(tmp_in_path).unlink(missing_ok=True)
        Path(tmp_out_path).unlink(missing_ok=True)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run MUSCLE alignment on sequence groups"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input FASTA file with sequence groups",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Output alignment file (default: stdout)",
    )
    parser.add_argument(
        "--muscle-path",
        default='muscle',
        help="Path to MUSCLE executable (default: muscle)",
    )
    parser.add_argument(
        "--max-iters",
        type=int,
        default=16,
        help="Maximum iterations (default: 16)",
    )
    parser.add_argument(
        "--format",
        choices=['fasta', 'clw'],
        default='clw',
        help="Output format (default: clw/clustal)",
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

    # Check MUSCLE availability
    try:
        result = subprocess.run(
            [args.muscle_path, '-version'],
            capture_output=True,
            text=True,
        )
        if args.verbose:
            print(f"Using MUSCLE: {result.stdout.strip()}", file=sys.stderr)
    except FileNotFoundError:
        print(f"Error: MUSCLE not found at '{args.muscle_path}'", file=sys.stderr)
        print("Install MUSCLE or specify path with --muscle-path", file=sys.stderr)
        sys.exit(1)

    # Parse sequence groups
    groups = parse_sequence_groups(args.input_file)
    if args.verbose:
        print(f"Found {len(groups)} sequence groups", file=sys.stderr)

    # Run alignments
    out_handle = open(args.output, 'w') if args.output else sys.stdout

    try:
        for i, group in enumerate(groups, 1):
            if args.verbose:
                print(f"Aligning group {i}/{len(groups)}...", file=sys.stderr)

            alignment = run_muscle(
                group,
                args.muscle_path,
                args.max_iters,
                args.format,
            )

            if alignment:
                out_handle.write(alignment)
                out_handle.write("\n")

    finally:
        if args.output:
            out_handle.close()

    if args.verbose:
        print(f"Completed {len(groups)} alignments", file=sys.stderr)


if __name__ == "__main__":
    main()
