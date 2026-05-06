#!/usr/bin/env python3
"""
Load C. tropicalis genomic DNA sequences (with introns) from genomic FASTA.

This script extracts genomic DNA sequences for each gene using coordinates
from the GFF file and loads them into the database.

Usage:
    python load_genomic_sequences.py --gff FILE --genomic FILE [--dry-run]

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
ADMIN_USER = os.getenv("ADMIN_USER", "cgdadmin").upper()

ORGANISM_NAME = "Candida tropicalis MYA-3404"
SOURCE = "C. tropicalis MYA-3404"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

COMPLEMENT = str.maketrans('ATCGatcg', 'TAGCtagc')


def reverse_complement(seq: str) -> str:
    """Return reverse complement of DNA sequence."""
    return seq.translate(COMPLEMENT)[::-1]


def parse_genomic_fasta(fasta_file: Path) -> Dict[str, str]:
    """Parse genomic FASTA into dict of {chromosome_id: sequence}."""
    sequences = {}
    current_id = None
    current_seq = []

    with open(fasta_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith('>'):
                if current_id:
                    sequences[current_id] = ''.join(current_seq).upper()
                current_id = line[1:].split()[0]
                current_seq = []
            else:
                current_seq.append(line)

        if current_id:
            sequences[current_id] = ''.join(current_seq).upper()

    return sequences


def parse_gff_genes(gff_file: Path) -> Tuple[List[Dict], Dict[str, str]]:
    """
    Parse GFF file and extract gene features with coordinates.
    Also extract gene_id -> protein_id mapping from CDS features.
    """
    genes = []
    gene_to_protein = {}

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

            seqid, source, feature_type, start, end, score, strand, phase, attributes = fields

            attr_dict = {}
            for attr in attributes.split(';'):
                if '=' in attr:
                    key, value = attr.split('=', 1)
                    attr_dict[key.strip()] = value.strip()

            # Extract protein_id mapping from CDS features
            if feature_type == 'CDS':
                protein_id = attr_dict.get('protein_id', '')
                gene_id = attr_dict.get('gene_id', '')
                if protein_id and gene_id:
                    gene_to_protein[gene_id] = protein_id

            # Only process gene features
            if feature_type != 'gene':
                continue

            gene_id = attr_dict.get('ID', '').split(',')[0]
            if not gene_id:
                continue

            genes.append({
                'chromosome': seqid,
                'gene_id': gene_id,
                'start': int(start),
                'end': int(end),
                'strand': strand,
            })

    return genes, gene_to_protein


def ensure_code(session, table_name: str, col_name: str, code_value: str, description: str, dry_run: bool = False) -> bool:
    """Ensure a code value exists in the code table."""
    query = text(f"""
        SELECT code_no FROM {DB_SCHEMA}.code
        WHERE tab_name = :tab_name AND col_name = :col_name AND code_value = :code_value
    """)
    result = session.execute(query, {
        "tab_name": table_name, "col_name": col_name, "code_value": code_value
    }).first()

    if result:
        return True

    if dry_run:
        logger.info(f"[DRY RUN] Would create code for {table_name}.{col_name}: {code_value}")
        return True

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.code (tab_name, col_name, code_value, description, created_by)
        VALUES (:tab_name, :col_name, :code_value, :description, :created_by)
    """)
    session.execute(insert, {
        "tab_name": table_name, "col_name": col_name, "code_value": code_value,
        "description": description, "created_by": ADMIN_USER,
    })
    session.commit()
    logger.info(f"Created code: {table_name}.{col_name}: {code_value}")
    return True


def get_organism_and_genome(session) -> Tuple[int, int]:
    """Get organism_no and genome_version_no for C. tropicalis."""
    query = text(f"SELECT organism_no FROM {DB_SCHEMA}.organism WHERE organism_name = :name")
    result = session.execute(query, {"name": ORGANISM_NAME}).first()
    if not result:
        raise ValueError(f"Organism not found: {ORGANISM_NAME}")
    organism_no = result[0]

    query = text(f"""
        SELECT genome_version_no FROM {DB_SCHEMA}.genome_version
        WHERE organism_no = :org_no AND is_ver_current = 'Y'
    """)
    result = session.execute(query, {"org_no": organism_no}).first()
    if not result:
        raise ValueError(f"No genome version for organism_no={organism_no}")

    return organism_no, result[0]


def get_feature_no(session, feature_name: str) -> Optional[int]:
    """Get feature_no by feature_name."""
    query = text(f"SELECT feature_no FROM {DB_SCHEMA}.feature WHERE feature_name = :name")
    result = session.execute(query, {"name": feature_name}).first()
    return result[0] if result else None


def sequence_exists(session, feature_no: int, seq_type: str) -> bool:
    """Check if sequence already exists."""
    query = text(f"""
        SELECT seq_no FROM {DB_SCHEMA}.seq
        WHERE feature_no = :fno AND seq_type = :stype AND is_seq_current = 'Y'
    """)
    result = session.execute(query, {"fno": feature_no, "stype": seq_type}).first()
    return result is not None


def create_sequence(session, feature_no: int, genome_version_no: int, seq_type: str,
                    residues: str, source: str) -> bool:
    """Create a sequence entry."""
    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.seq (
            feature_no, genome_version_no, seq_version, seq_type, source,
            is_seq_current, seq_length, residues, created_by
        ) VALUES (
            :fno, :gvno, SYSDATE, :stype, :source, 'Y', :len, :res, :created_by
        )
    """)
    session.execute(insert, {
        "fno": feature_no, "gvno": genome_version_no, "stype": seq_type,
        "source": source, "len": len(residues), "res": residues, "created_by": ADMIN_USER,
    })
    return True


def load_genomic_sequences(session, gff_file: Path, genomic_file: Path, dry_run: bool = False):
    """Load genomic DNA sequences for genes."""
    # Ensure seq_type code exists
    ensure_code(session, "SEQ", "SEQ_TYPE", "Genomic DNA", "Genomic DNA sequence with introns", dry_run)

    organism_no, genome_version_no = get_organism_and_genome(session)
    logger.info(f"Loading for organism_no={organism_no}, genome_version_no={genome_version_no}")

    logger.info(f"Parsing genomic FASTA: {genomic_file}")
    genomic_seqs = parse_genomic_fasta(genomic_file)
    logger.info(f"Found {len(genomic_seqs)} chromosomes/scaffolds")

    logger.info(f"Parsing GFF: {gff_file}")
    genes, gene_to_protein = parse_gff_genes(gff_file)
    logger.info(f"Found {len(genes)} genes, {len(gene_to_protein)} protein mappings")

    sequences_created = 0
    features_found = 0
    features_not_found = 0

    for i, gene in enumerate(genes):
        gene_id = gene['gene_id']
        protein_id = gene_to_protein.get(gene_id)

        # Find feature by protein_id or gene_id
        feature_no = None
        if protein_id:
            feature_no = get_feature_no(session, protein_id)
        if not feature_no:
            feature_no = get_feature_no(session, gene_id)

        if not feature_no:
            features_not_found += 1
            continue

        features_found += 1

        # Check if genomic sequence already exists
        if sequence_exists(session, feature_no, "Genomic DNA"):
            continue

        # Extract genomic sequence
        chrom = gene['chromosome']
        if chrom not in genomic_seqs:
            continue

        chrom_seq = genomic_seqs[chrom]
        start = gene['start'] - 1  # GFF is 1-based
        end = gene['end']
        genomic_seq = chrom_seq[start:end]

        # Reverse complement if on minus strand
        if gene['strand'] == '-':
            genomic_seq = reverse_complement(genomic_seq)

        if not dry_run:
            create_sequence(session, feature_no, genome_version_no, "Genomic DNA",
                            genomic_seq, SOURCE)
            sequences_created += 1

        if (i + 1) % 500 == 0:
            logger.info(f"Processed {i + 1}/{len(genes)} genes...")
            if not dry_run:
                session.commit()

    if not dry_run:
        session.commit()

    logger.info("=" * 60)
    logger.info(f"Features found: {features_found}")
    logger.info(f"Features not found: {features_not_found}")
    logger.info(f"Genomic sequences created: {sequences_created}")


def main():
    parser = argparse.ArgumentParser(description="Load C. tropicalis genomic DNA sequences")
    parser.add_argument("--gff", required=True, type=Path, help="GFF file")
    parser.add_argument("--genomic", required=True, type=Path, help="Genomic FASTA file")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Loading C. tropicalis genomic DNA sequences")
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("[DRY RUN MODE]")

    with SessionLocal() as session:
        load_genomic_sequences(session, args.gff, args.genomic, args.dry_run)

    logger.info("Done!")


if __name__ == "__main__":
    main()
