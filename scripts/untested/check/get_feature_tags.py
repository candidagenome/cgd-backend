#!/usr/bin/env python3
"""
Extract feature tags from EMBL files.

This script reads EMBL files and extracts specified tags (product, remarks,
etc.) for features, outputting them in a tabular format.

Original Perl: getFeatureTagsForOrfs.pl
Converted to Python: 2024
"""

import argparse
import re
import sys
from pathlib import Path

from Bio import SeqIO


def extract_feature_tags(
    embl_files: list[Path],
    tags: list[str],
    feature_types: list[str] = None,
    gene_pattern: str = None,
) -> list[dict]:
    """
    Extract tags from EMBL features.

    Args:
        embl_files: List of EMBL files to process
        tags: Tags to extract
        feature_types: Feature types to include (default: CDS)
        gene_pattern: Regex pattern to filter genes

    Returns:
        List of feature tag dicts
    """
    if feature_types is None:
        feature_types = ['CDS']

    results = []
    gene_re = re.compile(gene_pattern, re.IGNORECASE) if gene_pattern else None

    for embl_file in embl_files:
        print(f"Processing: {embl_file}", file=sys.stderr)

        try:
            for record in SeqIO.parse(str(embl_file), 'embl'):
                for feature in record.features:
                    if feature.type not in feature_types:
                        continue

                    # Get gene name
                    gene = None
                    if 'gene' in feature.qualifiers:
                        gene_values = feature.qualifiers['gene']
                        if gene_values:
                            gene = gene_values[0]

                    if not gene:
                        continue

                    # Apply gene pattern filter
                    if gene_re and not gene_re.search(gene):
                        continue

                    # Extract normalized gene name
                    match = re.search(r'(orf\d+\.\d+\.?\d*)', gene, re.IGNORECASE)
                    normalized_gene = match.group(1) if match else gene

                    # Extract requested tags
                    tag_values = {'gene': normalized_gene}

                    for tag in tags:
                        if tag in feature.qualifiers:
                            values = feature.qualifiers[tag]
                            if values:
                                # Join multiple values
                                tag_values[tag] = ' '.join(str(v) for v in values)
                            else:
                                tag_values[tag] = ''
                        else:
                            tag_values[tag] = ''

                    # Only include if at least one requested tag has a value
                    has_value = any(tag_values.get(t) for t in tags)
                    if has_value:
                        results.append({
                            'file': str(embl_file),
                            'record': record.id,
                            **tag_values,
                        })

        except Exception as e:
            print(f"Error parsing {embl_file}: {e}", file=sys.stderr)

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Extract feature tags from EMBL files"
    )
    parser.add_argument(
        "embl_files",
        type=Path,
        nargs="+",
        help="EMBL file(s) to process",
    )
    parser.add_argument(
        "--tags",
        nargs="+",
        default=['product', 'remarks'],
        help="Tags to extract (default: product remarks)",
    )
    parser.add_argument(
        "--feature-types",
        nargs="+",
        default=['CDS'],
        help="Feature types to include (default: CDS)",
    )
    parser.add_argument(
        "--gene-pattern",
        help="Regex pattern to filter genes (e.g., 'orf19')",
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
    for embl_file in args.embl_files:
        if not embl_file.exists():
            print(f"Error: File not found: {embl_file}", file=sys.stderr)
            sys.exit(1)

    # Extract tags
    results = extract_feature_tags(
        args.embl_files,
        args.tags,
        args.feature_types,
        args.gene_pattern,
    )

    # Output
    out_handle = open(args.output, 'w') if args.output else sys.stdout

    try:
        # Header
        header = ['ORFname'] + args.tags
        out_handle.write('\t'.join(header) + '\n\n')

        # Data rows
        for item in results:
            row = [item.get('gene', '')]
            for tag in args.tags:
                value = item.get(tag, '')
                # Quote values containing spaces
                if value and (' ' in value or '\t' in value):
                    value = f'"{value}"'
                row.append(value)
            out_handle.write('\t'.join(row) + '\n')

    finally:
        if args.output:
            out_handle.close()

    if args.verbose:
        print(f"Extracted tags for {len(results)} features", file=sys.stderr)


if __name__ == "__main__":
    main()
