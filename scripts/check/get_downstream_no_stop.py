#!/usr/bin/env python3
"""
Get downstream sequences for ORFs lacking stop codons.

This script extracts downstream genomic sequence for ORFs that don't
have a stop codon at their annotated end, useful for investigating
potential annotation errors.

Original Perl: getDownstreamForNoStopOrfs.pl
Converted to Python: 2024
"""

import argparse
import re
import sys
from pathlib import Path

from Bio import SeqIO
from Bio.Seq import Seq


# Alternative Yeast Nuclear genetic code
GENETIC_CODE = 12


def load_orf_list(input_file: Path) -> set[str]:
    """
    Load ORF names from file.

    Args:
        input_file: File with ORF names (one per line)

    Returns:
        Set of ORF names
    """
    orfs = set()
    pattern = re.compile(r'(orf\d+\.\d+\.?\d*)', re.IGNORECASE)

    with open(input_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            match = pattern.search(line)
            if match:
                orfs.add(match.group(1).lower())

    return orfs


def get_downstream_sequences(
    embl_files: list[Path],
    orf_list: set[str],
    downstream_bp: int = 15,
    genetic_code: int = GENETIC_CODE,
) -> list[dict]:
    """
    Get downstream sequences for specified ORFs.

    Args:
        embl_files: List of EMBL files to process
        orf_list: Set of ORF names to find
        downstream_bp: Number of downstream base pairs to include
        genetic_code: NCBI genetic code table number

    Returns:
        List of sequence info dicts
    """
    results = []

    for embl_file in embl_files:
        print(f"Processing: {embl_file}", file=sys.stderr)

        try:
            for record in SeqIO.parse(str(embl_file), 'embl'):
                for feature in record.features:
                    # Get gene name
                    if 'gene' not in feature.qualifiers:
                        continue

                    gene_values = feature.qualifiers['gene']
                    if not gene_values:
                        continue

                    gene = gene_values[0]

                    # Check if in our list
                    gene_lower = gene.lower()
                    match = re.search(r'(orf\d+\.\d+\.?\d*)', gene_lower)
                    if not match:
                        continue

                    orf_name = match.group(1)
                    if orf_name not in orf_list:
                        continue

                    # Get coordinates
                    start = int(feature.location.start)
                    end = int(feature.location.end)
                    strand = feature.location.strand

                    # Get sequences
                    try:
                        # Full unspliced sequence
                        full_seq = str(feature.extract(record.seq))

                        # Spliced sequence
                        spliced_seq = str(feature.extract(record.seq))

                        # Downstream sequence
                        if strand == 1:  # Forward
                            ds_start = end
                            ds_end = min(end + downstream_bp, len(record.seq))
                            downstream = str(record.seq[ds_start:ds_end])
                        else:  # Reverse
                            ds_start = max(start - downstream_bp, 0)
                            ds_end = start
                            downstream = str(record.seq[ds_start:ds_end].reverse_complement())

                        # Translate
                        seq_obj = Seq(spliced_seq)
                        translated = str(seq_obj.translate(table=genetic_code))

                        results.append({
                            'orf_name': orf_name,
                            'gene': gene,
                            'chromosome': record.id,
                            'start': start,
                            'end': end,
                            'strand': '+' if strand == 1 else '-',
                            'full_seq': full_seq,
                            'downstream_seq': downstream,
                            'translated': translated,
                        })

                    except Exception as e:
                        print(f"Error processing {gene}: {e}", file=sys.stderr)

        except Exception as e:
            print(f"Error parsing {embl_file}: {e}", file=sys.stderr)

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Get downstream sequences for ORFs lacking stop codons"
    )
    parser.add_argument(
        "orf_list",
        type=Path,
        help="File with ORF names to search for",
    )
    parser.add_argument(
        "embl_files",
        type=Path,
        nargs="+",
        help="EMBL file(s) to search",
    )
    parser.add_argument(
        "--downstream-bp",
        type=int,
        default=15,
        help="Number of downstream base pairs to include (default: 15)",
    )
    parser.add_argument(
        "--genetic-code",
        type=int,
        default=GENETIC_CODE,
        help=f"NCBI genetic code table (default: {GENETIC_CODE})",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Output file (default: stdout)",
    )
    parser.add_argument(
        "--format",
        choices=['fasta', 'text'],
        default='text',
        help="Output format (default: text)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    # Validate inputs
    if not args.orf_list.exists():
        print(f"Error: ORF list file not found: {args.orf_list}", file=sys.stderr)
        sys.exit(1)

    for embl_file in args.embl_files:
        if not embl_file.exists():
            print(f"Error: EMBL file not found: {embl_file}", file=sys.stderr)
            sys.exit(1)

    # Load ORF list
    orf_list = load_orf_list(args.orf_list)
    if args.verbose:
        print(f"Loaded {len(orf_list)} ORF names", file=sys.stderr)

    # Get sequences
    results = get_downstream_sequences(
        args.embl_files,
        orf_list,
        args.downstream_bp,
        args.genetic_code,
    )

    # Output
    out_handle = open(args.output, 'w') if args.output else sys.stdout

    try:
        if args.format == 'fasta':
            for item in results:
                out_handle.write(f">{item['orf_name']}_NA\n")
                out_handle.write(f"{item['full_seq']}\n")
                out_handle.write(f">{item['orf_name']}_downstream\n")
                out_handle.write(f"{item['downstream_seq']}\n")
                out_handle.write(f">{item['orf_name']}_translated\n")
                out_handle.write(f"{item['translated']}\n")
        else:
            for item in results:
                out_handle.write(f"########### {item['orf_name']} ##################\n\n")
                out_handle.write(f">{item['orf_name']}_NA\n{item['full_seq']}\n\n")
                out_handle.write(f">{item['orf_name']}_downstream\n{item['downstream_seq']}\n\n")
                out_handle.write(f">{item['orf_name']}_translated\n{item['translated']}\n\n")

    finally:
        if args.output:
            out_handle.close()

    if args.verbose:
        print(f"Found {len(results)} ORFs", file=sys.stderr)


if __name__ == "__main__":
    main()
