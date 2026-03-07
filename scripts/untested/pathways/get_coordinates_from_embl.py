#!/usr/bin/env python3
"""
Extract CDS coordinate information from EMBL files.

This program extracts the coordinate information for all CDS features
from EMBL files and outputs a file with gene names and CDS coordinates.

Original Perl: getCoordinatesFromEMBL.pl
Converted to Python: 2024

Usage:
    python get_coordinates_from_embl.py file1.embl file2.embl ...
"""

import argparse
import re
import sys
from pathlib import Path


def extract_cds_coordinates(embl_file: Path) -> list:
    """
    Extract CDS coordinates from an EMBL file.

    Args:
        embl_file: Path to EMBL file

    Returns:
        List of (gene_name, coordinates) tuples
    """
    results = []
    coords = None

    with open(embl_file) as f:
        for line in f:
            # Match CDS feature line
            cds_match = re.match(r'FT   CDS\s+(\S+)', line)
            if cds_match:
                coords = cds_match.group(1)

            # Match gene qualifier line
            gene_match = re.match(r'FT\s+/gene="(.+)"', line)
            if gene_match and coords:
                gene_name = gene_match.group(1)
                results.append((gene_name, coords))
                coords = None

    return results


def process_embl_files(embl_files: list[Path]) -> None:
    """
    Process multiple EMBL files and write coordinate files.

    Args:
        embl_files: List of EMBL file paths
    """
    for embl_file in embl_files:
        if not embl_file.exists():
            print(f"Warning: {embl_file} does not exist.", file=sys.stderr)
            continue

        print(f"Processing {embl_file}...")

        # Create output filename with .annot extension
        out_file = embl_file.with_suffix('.annot')

        results = extract_cds_coordinates(embl_file)

        with open(out_file, 'w') as f:
            for gene_name, coords in results:
                f.write(f"{gene_name}\t{coords}\n")

        print(f"  Created {out_file} with {len(results)} entries")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Extract CDS coordinates from EMBL files"
    )
    parser.add_argument(
        "embl_files",
        nargs='+',
        type=Path,
        help="One or more EMBL files to parse",
    )

    args = parser.parse_args()

    if not args.embl_files:
        print("Provide one or more EMBL files to parse.", file=sys.stderr)
        sys.exit(1)

    process_embl_files(args.embl_files)


if __name__ == "__main__":
    main()
