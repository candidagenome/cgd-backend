#!/usr/bin/env python3
"""
Get attribute combinations from GFF file.

This script extracts unique combinations of specific attributes from a GFF file.
Useful for analyzing annotation update tracking attributes.

Original Perl: get_attrib_combinations_from_GFF.pl (UMD_data_transfer)
Converted to Python: 2024

Usage:
    python get_gff_attrib_combinations.py input.gff
    python get_gff_attrib_combinations.py input.gff --attributes broad_update_type,broad_update_accepted
"""

import argparse
import sys
from collections import Counter
from pathlib import Path


# Default attributes to extract
DEFAULT_ATTRIBS = [
    'rel_2_to_3_to_tpa_unchanged',
    'broad_update_type',
    'broad_update_accepted',
]


def parse_gff_attributes(attr_string: str) -> dict[str, str]:
    """
    Parse GFF attribute string into dict.

    Args:
        attr_string: GFF column 9 attributes (key=value;key=value)

    Returns:
        Dict mapping attribute names to values
    """
    attrs = {}
    for item in attr_string.split(';'):
        if '=' in item:
            key, value = item.split('=', 1)
            attrs[key] = value
    return attrs


def get_attrib_combinations(
    gff_file: Path,
    attribs_to_read: list[str],
) -> Counter:
    """
    Extract attribute combinations from GFF file.

    Args:
        gff_file: Path to GFF file
        attribs_to_read: List of attribute names to extract

    Returns:
        Counter of attribute combinations
    """
    attrib_set = set(attribs_to_read)
    combinations: Counter = Counter()

    with open(gff_file, 'r') as f:
        for line in f:
            # Stop at FASTA section
            if line.startswith('#FASTA'):
                break

            # Skip comments and empty lines
            if line.startswith('#') or not line.strip():
                continue

            cols = line.strip().split('\t')
            if len(cols) < 9:
                continue

            # Parse attributes from column 9
            attrs = parse_gff_attributes(cols[8])

            # Extract only the attributes we care about
            combo_parts = []
            for key in attribs_to_read:
                if key in attrs:
                    combo_parts.append(f"{key} = {attrs[key]}")

            if combo_parts:
                combination = '\t'.join(combo_parts)
                combinations[combination] += 1

    return combinations


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Extract attribute combinations from GFF file"
    )
    parser.add_argument(
        "gff_file",
        type=Path,
        help="Input GFF file",
    )
    parser.add_argument(
        "--attributes", "-a",
        type=str,
        help=f"Comma-separated list of attributes to extract (default: {','.join(DEFAULT_ATTRIBS)})",
    )
    parser.add_argument(
        "--counts", "-c",
        action="store_true",
        help="Show counts for each combination",
    )

    args = parser.parse_args()

    if not args.gff_file.exists():
        print(f"Error: File not found: {args.gff_file}", file=sys.stderr)
        sys.exit(1)

    # Parse attributes to extract
    if args.attributes:
        attribs = [a.strip() for a in args.attributes.split(',')]
    else:
        attribs = DEFAULT_ATTRIBS

    # Get combinations
    combinations = get_attrib_combinations(args.gff_file, attribs)

    # Print results
    for combo, count in sorted(combinations.items()):
        if args.counts:
            print(f"{combo}\t({count})")
        else:
            print(combo)


if __name__ == "__main__":
    main()
