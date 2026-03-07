#!/usr/bin/env python3
"""
Swap ORF19 identifiers with gene names in PTools export files.

This script replaces ORF19 names in PTools export files with standard
gene names from the CGD chromosomal features file. The modified file
can then be reimported into Pathway Tools.

Original Perl: swap_gene_for_orf19.pl (Martha Arnaud, Gavin Sherlock, Jan 2008)
Converted to Python: 2024

Usage:
    python swap_gene_for_orf19.py --features chromosomal_feature.tab \\
        --input ptools_export.txt --output modified.txt
"""

import argparse
import sys
from pathlib import Path


def load_gene_mapping(features_file: Path) -> dict:
    """
    Load ORF19 to gene name mapping from chromosomal features file.

    Args:
        features_file: Path to chromosomal_feature.tab

    Returns:
        Dict mapping ORF19 IDs to gene names
    """
    gene_map = {}

    with open(features_file) as f:
        for line in f:
            line = line.rstrip('\n')
            parts = line.split('\t')

            if len(parts) >= 2:
                orf19 = parts[0]
                gene = parts[1] if parts[1] else orf19
            elif len(parts) == 1:
                orf19 = parts[0]
                gene = orf19
            else:
                continue

            gene_map[orf19] = gene

    return gene_map


def swap_names(
    input_file: Path,
    output_file: Path,
    gene_map: dict,
    name_column: int = 2,
) -> int:
    """
    Swap ORF19 names with gene names in PTools file.

    Args:
        input_file: Input PTools export file
        output_file: Output file path
        gene_map: Dict mapping ORF19 to gene names
        name_column: Column index containing names to swap (0-based)

    Returns:
        Number of swaps performed
    """
    swaps = 0

    with open(input_file) as f_in, open(output_file, 'w') as f_out:
        for line in f_in:
            line = line.rstrip('\n')
            fields = line.split('\t')

            # Perform swap if column exists and ID is in mapping
            if len(fields) > name_column:
                old_name = fields[name_column]
                if old_name in gene_map:
                    fields[name_column] = gene_map[old_name]
                    swaps += 1

            f_out.write('\t'.join(fields) + '\n')

    return swaps


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Swap ORF19 IDs with gene names in PTools files"
    )
    parser.add_argument(
        "--features",
        type=Path,
        required=True,
        help="Path to chromosomal_feature.tab file",
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Input PTools export file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output file (default: stdout)",
    )
    parser.add_argument(
        "--column",
        type=int,
        default=2,
        help="Column index containing names to swap (0-based, default: 2)",
    )

    args = parser.parse_args()

    # Validate input files
    if not args.features.exists():
        print(f"Error: Features file not found: {args.features}", file=sys.stderr)
        sys.exit(1)

    if not args.input.exists():
        print(f"Error: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    # Load gene mapping
    gene_map = load_gene_mapping(args.features)
    print(f"Loaded {len(gene_map)} gene mappings", file=sys.stderr)

    # Process file
    if args.output:
        swaps = swap_names(args.input, args.output, gene_map, args.column)
        print(f"Performed {swaps} name swaps, output written to {args.output}",
              file=sys.stderr)
    else:
        # Output to stdout
        with open(args.input) as f:
            for line in f:
                line = line.rstrip('\n')
                fields = line.split('\t')
                if len(fields) > args.column and fields[args.column] in gene_map:
                    fields[args.column] = gene_map[fields[args.column]]
                print('\t'.join(fields))


if __name__ == "__main__":
    main()
