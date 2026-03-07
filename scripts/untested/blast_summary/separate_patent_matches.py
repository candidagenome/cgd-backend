#!/usr/bin/env python3
"""
Separate GenBank patent vs non-patent matches from BLAST summary.

This script reads a blastSummary.tab file and separates GenBank matches
into Patent vs. non-Patent sequences based on accession prefixes.

Original Perl: separatePatentMatchesFromBlastSummary.pl
Converted to Python: 2024

Usage:
    python separate_patent_matches.py SPECIES blastSummary.tab
"""

import argparse
import re
import sys
from pathlib import Path

# Patent accession prefixes from GenBank
PATENT_PREFIXES = {
    'E', 'BD', 'DD', 'DI', 'DJ', 'DL', 'DM', 'FU', 'FV', 'FW', 'FZ',
    'GB', 'HV', 'HW', 'A', 'AX', 'CQ', 'CS', 'FB', 'GM', 'GN', 'HA',
    'HB', 'HC', 'HD', 'HH', 'HI', 'JA', 'JB', 'JC', 'JD', 'JE', 'I',
    'AR', 'DZ', 'EA', 'GC', 'GP', 'GV', 'GX', 'GY', 'GZ', 'HJ', 'HK', 'HL',
}


def separate_patent_matches(
    species: str,
    input_file: Path,
    output_dir: Path = None,
) -> dict:
    """
    Separate BLAST matches into patent and non-patent files.

    Args:
        species: Species name for output filenames
        input_file: Path to blastSummary.tab file
        output_dir: Output directory (default: current directory)

    Returns:
        Stats dict with counts
    """
    output_dir = output_dir or Path('.')

    patents_file = output_dir / f"{species}_GBpatents_load.tab"
    non_patents_file = output_dir / f"{species}_GBnonpatents_load.tab"

    stats = {
        'total_matches': 0,
        'good_matches': 0,
        'patent_matches': 0,
        'non_patent_matches': 0,
    }

    with open(input_file) as f_in, \
         open(patents_file, 'w') as f_patents, \
         open(non_patents_file, 'w') as f_non_patents:

        for line_num, line in enumerate(f_in, 1):
            # Skip header
            if line_num == 1:
                continue

            line = line.rstrip('\n')
            fields = line.split('\t')

            if len(fields) < 9:
                continue

            stats['total_matches'] += 1

            query = fields[0]
            match = fields[2]
            match_class = fields[8]

            # Only process good matches
            if match_class != 'Good_match':
                continue

            stats['good_matches'] += 1

            # Parse accession from match (format: db|accession)
            match_parts = match.split('|')
            if len(match_parts) >= 2:
                accession = match_parts[1]
            else:
                accession = match

            # Extract prefix (letters at start of accession)
            prefix_match = re.match(r'^([A-Z]+)', accession)
            if not prefix_match:
                continue

            prefix = prefix_match.group(1)

            # Determine if patent or not
            if prefix in PATENT_PREFIXES:
                f_patents.write(f"{query}\t{accession}\n")
                stats['patent_matches'] += 1
            else:
                f_non_patents.write(f"{query}\t{accession}\n")
                stats['non_patent_matches'] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Separate GenBank patent vs non-patent matches from BLAST summary"
    )
    parser.add_argument(
        "species",
        help="Species name for output filenames",
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="blastSummary.tab file to process",
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=Path,
        default=Path('.'),
        help="Output directory (default: current directory)",
    )

    args = parser.parse_args()

    if not args.input_file.exists():
        print(f"Error: File not found: {args.input_file}", file=sys.stderr)
        sys.exit(1)

    stats = separate_patent_matches(args.species, args.input_file, args.output_dir)

    print(f"Processed {stats['total_matches']} total matches")
    print(f"  Good matches: {stats['good_matches']}")
    print(f"  Patent matches: {stats['patent_matches']}")
    print(f"  Non-patent matches: {stats['non_patent_matches']}")
    print(f"Output files:")
    print(f"  {args.species}_GBpatents_load.tab")
    print(f"  {args.species}_GBnonpatents_load.tab")


if __name__ == "__main__":
    main()
