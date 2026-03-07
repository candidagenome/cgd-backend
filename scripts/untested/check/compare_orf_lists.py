#!/usr/bin/env python3
"""
Compare two ORF lists and report differences.

This script compares two files containing ORF names and reports:
- ORFs present in file1 but missing from file2
- ORFs present in file2 but missing from file1
- ORFs present in both files

ORF names are extracted using regex pattern: orf\\d+\\.\\d+\\.?\\d*

Original Perl: compareORFlistsAndReportDiffs.pl
Converted to Python: 2024
"""

import argparse
import re
import sys
from pathlib import Path


def extract_orfs(filepath: Path) -> set:
    """
    Extract ORF names from a file.

    Args:
        filepath: Path to input file

    Returns:
        Set of ORF names found
    """
    orfs = set()
    pattern = re.compile(r'(orf\d+\.\d+\.?\d*)', re.IGNORECASE)

    with open(filepath) as f:
        for line in f:
            match = pattern.search(line)
            if match:
                orfs.add(match.group(1).lower())

    return orfs


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Compare two ORF lists and report differences"
    )
    parser.add_argument(
        "file1",
        type=Path,
        help="First ORF list file",
    )
    parser.add_argument(
        "file2",
        type=Path,
        help="Second ORF list file",
    )
    parser.add_argument(
        "--show-common",
        action="store_true",
        help="Show ORFs present in both files",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Only show counts, not individual ORFs",
    )

    args = parser.parse_args()

    # Validate files exist
    if not args.file1.exists():
        print(f"Error: File not found: {args.file1}", file=sys.stderr)
        sys.exit(1)
    if not args.file2.exists():
        print(f"Error: File not found: {args.file2}", file=sys.stderr)
        sys.exit(1)

    # Extract ORFs from both files
    orfs1 = extract_orfs(args.file1)
    orfs2 = extract_orfs(args.file2)

    # Calculate differences
    only_in_1 = orfs1 - orfs2
    only_in_2 = orfs2 - orfs1
    in_both = orfs1 & orfs2

    # Report results
    print(f"Missing in {args.file1.name} (present only in {args.file2.name}):")
    print(f"  Count: {len(only_in_2)}")
    if not args.summary_only and only_in_2:
        print()
        for orf in sorted(only_in_2):
            print(f"  {orf}")
    print()

    print(f"Missing in {args.file2.name} (present only in {args.file1.name}):")
    print(f"  Count: {len(only_in_1)}")
    if not args.summary_only and only_in_1:
        print()
        for orf in sorted(only_in_1):
            print(f"  {orf}")
    print()

    if args.show_common or args.summary_only:
        print(f"Present in both files:")
        print(f"  Count: {len(in_both)}")
        if args.show_common and not args.summary_only and in_both:
            print()
            for orf in sorted(in_both):
                print(f"  {orf}")
        print()

    # Summary
    print("=" * 40)
    print(f"Total in {args.file1.name}: {len(orfs1)}")
    print(f"Total in {args.file2.name}: {len(orfs2)}")
    print(f"Common: {len(in_both)}")
    print(f"Unique to {args.file1.name}: {len(only_in_1)}")
    print(f"Unique to {args.file2.name}: {len(only_in_2)}")


if __name__ == "__main__":
    main()
