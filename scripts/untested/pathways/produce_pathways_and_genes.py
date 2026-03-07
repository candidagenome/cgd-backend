#!/usr/bin/env python3
"""
Process PTools pathways.col file and generate pathways and genes download file.

This script processes the Pathway Tools flatfile (pathways.col) and generates
a tab-delimited file for download by users (pathwaysAndGenes.tab).

Original Perl: producePathwaysAndGenes.pl (Martha Arnaud, Prachi Shah, April 2008)
Converted to Python: 2024

Usage:
    python produce_pathways_and_genes.py --input pathways.col --output pathwaysAndGenes.tab
"""

import argparse
import re
import sys
from pathlib import Path


def process_pathways_file(input_file: Path, output_file: Path) -> dict:
    """
    Process pathways.col file and generate tab-delimited output.

    Args:
        input_file: Path to pathways.col input file
        output_file: Path to output file

    Returns:
        Stats dict with processing counts
    """
    stats = {
        'pathways_processed': 0,
        'genes_found': 0,
    }

    header_found = False
    gene_id_col = None

    with open(input_file) as f_in, open(output_file, 'w') as f_out:
        # Write header
        f_out.write("UNIQUE-ID\tNAME\tGENE-NAMES\tGENE-IDS\n")

        for line in f_in:
            line = line.rstrip('\n')

            # Skip until we find the header line
            if 'UNIQUE-ID' in line:
                header_found = True
                header_cols = line.split('\t')

                # Find GENE-ID column
                for i, col in enumerate(header_cols):
                    if 'GENE-ID' in col:
                        gene_id_col = i
                        break

                continue

            if not header_found:
                continue

            # Process data lines
            col_values = line.split('\t')
            if len(col_values) < 2:
                continue

            pathway_id = col_values[0]
            pathway_name = col_values[1]

            # Extract gene names and IDs
            gene_name_col = gene_id_col - 1 if gene_id_col else 2

            genes = col_values[2:gene_name_col + 1] if gene_name_col > 2 else []
            gene_ids = col_values[gene_id_col:] if gene_id_col else []

            # Build colon-separated lists
            gene_list = ':'.join(g for g in genes if g)
            gene_id_list = ':'.join(g for g in gene_ids if g)

            # Make ORF19 lowercase
            gene_id_list = gene_id_list.replace('ORF19', 'orf19')

            # Write output
            f_out.write(f"{pathway_id}\t{pathway_name}\t{gene_list}\t{gene_id_list}\n")

            stats['pathways_processed'] += 1
            stats['genes_found'] += len([g for g in genes if g])

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Process PTools pathways.col and generate download file"
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to pathways.col input file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to output pathwaysAndGenes.tab file",
    )

    args = parser.parse_args()

    # Validate input
    if not args.input.exists():
        print(f"Error: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    # Process file
    stats = process_pathways_file(args.input, args.output)

    print(f"Processed {stats['pathways_processed']} pathways")
    print(f"Found {stats['genes_found']} gene associations")
    print(f"Output written to {args.output}")


if __name__ == "__main__":
    main()
