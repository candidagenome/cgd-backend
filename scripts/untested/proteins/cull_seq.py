#!/usr/bin/env python3
"""
Remove sequences from a file based on a list.

This script filters sequences from a target file, removing any that
match IDs in the provided list file. Useful for removing processed
sequences from loading files.

Original Perl: cull_seq.pl
Converted to Python: 2024
"""

import argparse
import sys
from pathlib import Path


def load_cull_list(list_file: Path) -> set[str]:
    """
    Load sequence IDs to cull from file.

    Args:
        list_file: File with one ID per line

    Returns:
        Set of IDs to remove
    """
    ids = set()

    with open(list_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                # Extract first whitespace-delimited field
                parts = line.split()
                if parts:
                    ids.add(parts[0])

    return ids


def cull_sequences(
    target_file: Path,
    cull_ids: set[str],
    output_file: Path = None,
) -> tuple[int, int]:
    """
    Filter sequences from target file.

    Args:
        target_file: File to filter (tab-delimited, ID in first column)
        cull_ids: Set of IDs to remove
        output_file: Output file (stdout if None)

    Returns:
        Tuple of (total_lines, kept_lines)
    """
    total = 0
    kept = 0

    out_handle = open(output_file, 'w') if output_file else sys.stdout

    try:
        with open(target_file) as f:
            for line in f:
                total += 1
                stripped = line.rstrip('\n')

                # Extract first field (ID)
                parts = stripped.split()
                if not parts:
                    out_handle.write(line)
                    kept += 1
                    continue

                candidate_id = parts[0]

                if candidate_id not in cull_ids:
                    out_handle.write(line)
                    kept += 1

    finally:
        if output_file:
            out_handle.close()

    return total, kept


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Remove sequences from a file based on a list"
    )
    parser.add_argument(
        "list_file",
        type=Path,
        help="File with IDs to remove (one per line)",
    )
    parser.add_argument(
        "target_file",
        type=Path,
        help="Target file to filter",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Output file (default: stdout)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    # Validate inputs
    if not args.list_file.exists():
        print(f"Error: List file not found: {args.list_file}", file=sys.stderr)
        sys.exit(1)

    if not args.target_file.exists():
        print(f"Error: Target file not found: {args.target_file}", file=sys.stderr)
        sys.exit(1)

    # Load cull list
    cull_ids = load_cull_list(args.list_file)

    if args.verbose:
        print(f"Loaded {len(cull_ids)} IDs to remove", file=sys.stderr)

    # Filter sequences
    total, kept = cull_sequences(args.target_file, cull_ids, args.output)

    if args.verbose:
        removed = total - kept
        print(f"Total lines: {total}", file=sys.stderr)
        print(f"Kept: {kept}", file=sys.stderr)
        print(f"Removed: {removed}", file=sys.stderr)


if __name__ == "__main__":
    main()
