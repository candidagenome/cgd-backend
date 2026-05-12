#!/usr/bin/env python3
"""
Rename protein FASTA headers from protein IDs to gene IDs.

This script creates a new protein FASTA file with gene IDs (CTRG_*) as
sequence names instead of protein IDs (EER*), for use with JBrowse2.

Usage:
    python rename_protein_fasta.py --fasta FILE --gff FILE --output FILE

Example:
    python rename_protein_fasta.py \
        --fasta ~/C_tropicalis/proteins.fasta \
        --gff ~/Ctrop_liftover3_sorted.gff \
        --output ~/C_tropicalis/proteins_by_gene_id.fasta
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_gff_protein_mapping(gff_file: Path) -> Dict[str, str]:
    """Parse GFF file to get protein_id -> gene_id mapping."""
    protein_to_gene = {}

    with open(gff_file, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue
            line = line.strip()
            if not line:
                continue

            fields = line.split('\t')
            if len(fields) < 9:
                continue

            feature_type = fields[2]
            attributes = fields[8]

            if feature_type == 'CDS':
                attr_dict = {}
                for attr in attributes.split(';'):
                    if '=' in attr:
                        key, value = attr.split('=', 1)
                        attr_dict[key.strip()] = value.strip()

                protein_id = attr_dict.get('protein_id', '')
                gene_id = attr_dict.get('gene_id', '')
                if protein_id and gene_id:
                    protein_to_gene[protein_id] = gene_id

    return protein_to_gene


def rename_fasta_headers(
    input_fasta: Path,
    output_fasta: Path,
    protein_to_gene: Dict[str, str]
):
    """
    Create new FASTA with gene IDs as sequence names.

    Args:
        input_fasta: Original protein FASTA with protein IDs
        output_fasta: Output FASTA with gene IDs
        protein_to_gene: Mapping of protein_id -> gene_id
    """
    renamed_count = 0
    unchanged_count = 0
    total_sequences = 0

    with open(input_fasta, 'r') as infile, open(output_fasta, 'w') as outfile:
        for line in infile:
            if line.startswith('>'):
                total_sequences += 1
                # Extract protein ID from header (first word after >)
                header = line[1:].strip()
                protein_id = header.split()[0]

                # Look up gene ID
                gene_id = protein_to_gene.get(protein_id)

                if gene_id:
                    # Replace protein ID with gene ID, keep rest of header
                    rest_of_header = header[len(protein_id):]
                    new_header = f">{gene_id}{rest_of_header}\n"
                    outfile.write(new_header)
                    renamed_count += 1
                else:
                    # Keep original header if no mapping found
                    outfile.write(line)
                    unchanged_count += 1
                    logger.debug(f"No mapping for protein: {protein_id}")
            else:
                # Write sequence lines unchanged
                outfile.write(line)

    return total_sequences, renamed_count, unchanged_count


def main():
    parser = argparse.ArgumentParser(
        description="Rename protein FASTA headers from protein IDs to gene IDs"
    )
    parser.add_argument(
        "--fasta", required=True, type=Path,
        help="Input protein FASTA file (with protein IDs)"
    )
    parser.add_argument(
        "--gff", required=True, type=Path,
        help="GFF file for protein_id to gene_id mapping"
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Output protein FASTA file (with gene IDs)"
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Renaming protein FASTA headers")
    logger.info("=" * 60)

    # Parse GFF for mapping
    logger.info(f"Parsing GFF for protein mapping: {args.gff}")
    protein_to_gene = parse_gff_protein_mapping(args.gff)
    logger.info(f"Found {len(protein_to_gene)} protein-to-gene mappings")

    # Rename FASTA headers
    logger.info(f"Processing FASTA: {args.fasta}")
    total, renamed, unchanged = rename_fasta_headers(
        args.fasta, args.output, protein_to_gene
    )

    logger.info("=" * 60)
    logger.info(f"Total sequences: {total}")
    logger.info(f"Renamed: {renamed}")
    logger.info(f"Unchanged (no mapping): {unchanged}")
    logger.info(f"Output written to: {args.output}")
    logger.info("Done!")


if __name__ == "__main__":
    main()
