#!/usr/bin/env python3
"""
Check EMBL files for blank/missing entries.

This script reads EMBL files and reports features with missing
required fields like gene name or contig information.

Original Perl: check_blankEntriesInEMBL.pl
Converted to Python: 2024
"""

import argparse
import sys
from pathlib import Path

from Bio import SeqIO


def check_embl_entries(
    input_files: list[Path],
    required_tags: list[str] = None,
    feature_types: list[str] = None,
) -> list[dict]:
    """
    Check EMBL files for blank entries.

    Args:
        input_files: List of EMBL files to check
        required_tags: Tags that must have values (default: gene)
        feature_types: Feature types to check (default: CDS)

    Returns:
        List of issues found
    """
    if required_tags is None:
        required_tags = ['gene']
    if feature_types is None:
        feature_types = ['CDS']

    issues = []

    for input_file in input_files:
        print(f"Processing: {input_file}", file=sys.stderr)

        try:
            for record in SeqIO.parse(str(input_file), 'embl'):
                for feature in record.features:
                    if feature.type not in feature_types:
                        continue

                    feature_info = {
                        'file': str(input_file),
                        'record': record.id,
                        'location': str(feature.location),
                        'type': feature.type,
                    }

                    # Get gene name if available
                    gene_name = None
                    if 'gene' in feature.qualifiers:
                        gene_values = feature.qualifiers['gene']
                        if gene_values:
                            gene_name = gene_values[0]
                    feature_info['gene'] = gene_name

                    # Check each required tag
                    for tag in required_tags:
                        if tag not in feature.qualifiers:
                            issues.append({
                                **feature_info,
                                'issue': f"Missing tag: {tag}",
                            })
                        else:
                            values = feature.qualifiers[tag]
                            if not values:
                                issues.append({
                                    **feature_info,
                                    'issue': f"Empty tag: {tag}",
                                })
                            else:
                                # Check for placeholder values
                                value = values[0] if values else ''
                                if not value or value == '' or 'no_value' in value.lower():
                                    issues.append({
                                        **feature_info,
                                        'issue': f"Blank/invalid value for tag: {tag}",
                                    })

        except Exception as e:
            issues.append({
                'file': str(input_file),
                'issue': f"Parse error: {e}",
            })

    return issues


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Check EMBL files for blank/missing entries"
    )
    parser.add_argument(
        "input_files",
        type=Path,
        nargs="+",
        help="Input EMBL file(s)",
    )
    parser.add_argument(
        "--required-tags",
        nargs="+",
        default=['gene'],
        help="Tags that must have values (default: gene)",
    )
    parser.add_argument(
        "--feature-types",
        nargs="+",
        default=['CDS'],
        help="Feature types to check (default: CDS)",
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

    # Validate input files
    for input_file in args.input_files:
        if not input_file.exists():
            print(f"Error: File not found: {input_file}", file=sys.stderr)
            sys.exit(1)

    # Check files
    issues = check_embl_entries(
        args.input_files,
        args.required_tags,
        args.feature_types,
    )

    # Output results
    out_handle = open(args.output, 'w') if args.output else sys.stdout

    try:
        out_handle.write(f"# Issues found: {len(issues)}\n")
        out_handle.write("# File\tGene\tLocation\tIssue\n")

        for issue in issues:
            gene = issue.get('gene', 'N/A') or 'N/A'
            location = issue.get('location', 'N/A')
            out_handle.write(
                f"{issue['file']}\t{gene}\t{location}\t{issue['issue']}\n"
            )

    finally:
        if args.output:
            out_handle.close()

    if args.verbose:
        print(f"Found {len(issues)} issues", file=sys.stderr)


if __name__ == "__main__":
    main()
