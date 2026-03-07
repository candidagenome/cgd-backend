#!/usr/bin/env python3
"""
Grep a list of IDs and show which files contain matches.

This script takes a list of gene IDs and searches for each in specified files,
reporting which file(s) contain each ID. Useful for tracking which genes
are accounted for in release files.

Original Perl: grep_list_and_get_match_files.pl (UMD_data_transfer)
Converted to Python: 2024

Usage:
    python grep_list_match_files.py genes.txt release.gff rejected.txt
    python grep_list_match_files.py genes.txt file1.txt file2.txt file3.txt
"""

import argparse
import sys
from pathlib import Path


def find_id_in_files(id_str: str, files: list[Path]) -> list[Path]:
    """
    Find which files contain the given ID.

    Args:
        id_str: ID string to search for
        files: List of files to search

    Returns:
        List of files containing the ID
    """
    matching_files = []

    for file_path in files:
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    if id_str in line:
                        matching_files.append(file_path)
                        break
        except IOError as e:
            print(f"Warning: Could not read {file_path}: {e}", file=sys.stderr)

    return matching_files


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Find which files contain each ID from a list"
    )
    parser.add_argument(
        "id_list",
        type=Path,
        help="File containing list of IDs (one per line)",
    )
    parser.add_argument(
        "search_files",
        type=Path,
        nargs='+',
        help="Files to search for IDs",
    )
    parser.add_argument(
        "--missing-only", "-m",
        action="store_true",
        help="Only show IDs not found in any file",
    )
    parser.add_argument(
        "--found-only", "-f",
        action="store_true",
        help="Only show IDs found in at least one file",
    )

    args = parser.parse_args()

    # Validate inputs
    if not args.id_list.exists():
        print(f"Error: ID list file not found: {args.id_list}", file=sys.stderr)
        sys.exit(1)

    for f in args.search_files:
        if not f.exists():
            print(f"Error: Search file not found: {f}", file=sys.stderr)
            sys.exit(1)

    # Process IDs
    total_ids = 0
    found_count = 0
    missing_count = 0

    with open(args.id_list, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            total_ids += 1
            id_str = line

            matching_files = find_id_in_files(id_str, args.search_files)

            if matching_files:
                found_count += 1
                if not args.missing_only:
                    file_names = ', '.join(str(f.name) for f in matching_files)
                    print(f"{id_str} -- {file_names}")
            else:
                missing_count += 1
                if not args.found_only:
                    print(id_str)

    # Print summary to stderr
    print(f"\nSummary: {found_count}/{total_ids} IDs found, "
          f"{missing_count} missing", file=sys.stderr)


if __name__ == "__main__":
    main()
