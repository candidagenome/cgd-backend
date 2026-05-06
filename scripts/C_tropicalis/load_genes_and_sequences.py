#!/usr/bin/env python3
"""
Load C. tropicalis genes (features) and sequences into database.

This script loads:
1. Features (genes/ORFs) from GFF file
2. Protein sequences from FASTA file
3. Feature locations on chromosomes

Usage:
    python load_genes_and_sequences.py --gff FILE --proteins FILE [--dry-run]

Data files needed:
    - GFF file with gene annotations (Ctrop_liftover3_sorted.gff)
    - Protein FASTA file (C_tropicalis_MYA-3404_proteins.fasta)

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from sqlalchemy import text

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
ADMIN_USER = os.getenv("ADMIN_USER", "cgdadmin").upper()

# C. tropicalis configuration
ORGANISM_NAME = "Candida tropicalis MYA-3404"
SOURCE = "C. tropicalis MYA-3404"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def ensure_source_code(session, source: str, table_name: str, dry_run: bool = False) -> bool:
    """Ensure the source value exists in the code table."""
    query = text(f"""
        SELECT code_no
        FROM {DB_SCHEMA}.code
        WHERE tab_name = :tab_name
        AND col_name = 'SOURCE'
        AND code_value = :code_value
    """)
    result = session.execute(query, {"tab_name": table_name, "code_value": source}).first()

    if result:
        logger.info(f"Code already exists for {table_name}.SOURCE: {source}")
        return True

    if dry_run:
        logger.info(f"[DRY RUN] Would create code entry for {table_name}.SOURCE: {source}")
        return True

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.code (
            tab_name, col_name, code_value, description, created_by
        ) VALUES (
            :tab_name, 'SOURCE', :code_value, :description, :created_by
        )
    """)
    session.execute(insert, {
        "tab_name": table_name,
        "code_value": source,
        "description": f"Source for {ORGANISM_NAME} data",
        "created_by": ADMIN_USER,
    })
    session.commit()
    logger.info(f"Created code entry for {table_name}.SOURCE: {source}")
    return True


def parse_fasta(fasta_file: Path) -> Dict[str, str]:
    """Parse FASTA file into dict of {id: sequence}."""
    sequences = {}
    current_id = None
    current_seq = []

    with open(fasta_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith('>'):
                if current_id:
                    sequences[current_id] = ''.join(current_seq)
                # Extract ID (first word after >)
                current_id = line[1:].split()[0]
                current_seq = []
            else:
                current_seq.append(line)

        if current_id:
            sequences[current_id] = ''.join(current_seq)

    return sequences


def parse_gff(gff_file: Path) -> List[Dict]:
    """Parse GFF file and extract gene features."""
    genes = []

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

            # Only process gene features
            if feature_type != 'gene':
                continue

            # Parse attributes
            attr_dict = {}
            for attr in attributes.split(';'):
                if '=' in attr:
                    key, value = attr.split('=', 1)
                    attr_dict[key.strip()] = value.strip()

            gene_id = attr_dict.get('ID', '').replace('ID=', '').split(',')[0]
            if not gene_id:
                continue

            genes.append({
                'chromosome': seqid,
                'gene_id': gene_id,
                'start': int(start),
                'end': int(end),
                'strand': 'W' if strand == '+' else 'C',
                'source': source,
            })

    return genes


def get_organism_no(session) -> int:
    """Get organism_no for C. tropicalis."""
    query = text(f"""
        SELECT organism_no
        FROM {DB_SCHEMA}.organism
        WHERE common_name = :common_name
    """)
    result = session.execute(query, {"common_name": ORGANISM_NAME}).first()
    if not result:
        raise ValueError(f"Organism not found: {ORGANISM_NAME}. Run load_organism.py first.")
    return result[0]


def get_genome_version_no(session, organism_no: int) -> int:
    """Get current genome_version_no for organism."""
    query = text(f"""
        SELECT genome_version_no
        FROM {DB_SCHEMA}.genome_version
        WHERE organism_no = :organism_no
        AND is_ver_current = 'Y'
    """)
    result = session.execute(query, {"organism_no": organism_no}).first()
    if not result:
        raise ValueError(f"No current genome version found for organism_no={organism_no}")
    return result[0]


def create_feature(
    session,
    organism_no: int,
    feature_name: str,
    dbxref_id: str,
    feature_type: str = "ORF",
    headline: str = None,
    dry_run: bool = False
) -> Optional[int]:
    """Create a feature entry."""
    # Check if feature exists
    query = text(f"""
        SELECT feature_no
        FROM {DB_SCHEMA}.feature
        WHERE feature_name = :feature_name
    """)
    result = session.execute(query, {"feature_name": feature_name}).first()

    if result:
        return result[0]

    if dry_run:
        return None

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.feature (
            organism_no, feature_name, dbxref_id, feature_type, source, headline, created_by
        ) VALUES (
            :organism_no, :feature_name, :dbxref_id, :feature_type, :source, :headline, :created_by
        )
    """)
    session.execute(insert, {
        "organism_no": organism_no,
        "feature_name": feature_name,
        "dbxref_id": dbxref_id,
        "feature_type": feature_type,
        "source": SOURCE,
        "headline": headline,
        "created_by": ADMIN_USER,
    })

    result = session.execute(query, {"feature_name": feature_name}).first()
    return result[0] if result else None


def create_sequence(
    session,
    feature_no: int,
    genome_version_no: int,
    seq_type: str,
    residues: str,
    dry_run: bool = False
) -> Optional[int]:
    """Create a sequence entry."""
    # Check if sequence exists
    query = text(f"""
        SELECT seq_no
        FROM {DB_SCHEMA}.seq
        WHERE feature_no = :feature_no
        AND seq_type = :seq_type
        AND is_seq_current = 'Y'
    """)
    result = session.execute(query, {
        "feature_no": feature_no,
        "seq_type": seq_type,
    }).first()

    if result:
        return result[0]

    if dry_run:
        return None

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.seq (
            feature_no, genome_version_no, seq_version, seq_type, source,
            is_seq_current, seq_length, residues, created_by
        ) VALUES (
            :feature_no, :genome_version_no, SYSDATE, :seq_type, :source,
            'Y', :seq_length, :residues, :created_by
        )
    """)
    session.execute(insert, {
        "feature_no": feature_no,
        "genome_version_no": genome_version_no,
        "seq_type": seq_type,
        "source": SOURCE,
        "seq_length": len(residues),
        "residues": residues,
        "created_by": ADMIN_USER,
    })

    result = session.execute(query, {
        "feature_no": feature_no,
        "seq_type": seq_type,
    }).first()
    return result[0] if result else None


def load_genes_and_proteins(
    session,
    gff_file: Path,
    proteins_file: Path,
    dry_run: bool = False
):
    """Load genes from GFF and protein sequences from FASTA."""
    # Ensure source code exists for FEATURE and SEQ tables
    ensure_source_code(session, SOURCE, "FEATURE", dry_run)
    ensure_source_code(session, SOURCE, "SEQ", dry_run)

    organism_no = get_organism_no(session)
    genome_version_no = get_genome_version_no(session, organism_no)

    logger.info(f"Loading genes for organism_no={organism_no}, genome_version_no={genome_version_no}")

    # Parse input files
    logger.info(f"Parsing GFF file: {gff_file}")
    genes = parse_gff(gff_file)
    logger.info(f"Found {len(genes)} genes in GFF")

    logger.info(f"Parsing protein FASTA: {proteins_file}")
    proteins = parse_fasta(proteins_file)
    logger.info(f"Found {len(proteins)} protein sequences")

    # Load genes and proteins
    features_created = 0
    sequences_created = 0

    for i, gene in enumerate(genes):
        gene_id = gene['gene_id']

        # Create feature
        # Use gene_id as both feature_name and dbxref_id for now
        # The protein ID mapping may need adjustment based on your GFF format
        feature_no = create_feature(
            session,
            organism_no,
            feature_name=gene_id,
            dbxref_id=f"CTROP:{gene_id}",
            feature_type="ORF",
            dry_run=dry_run
        )

        if feature_no and not dry_run:
            features_created += 1

            # Try to find matching protein sequence
            # This assumes protein IDs can be mapped from gene IDs
            # You may need to adjust this logic based on your data
            protein_seq = proteins.get(gene_id)
            if protein_seq:
                seq_no = create_sequence(
                    session,
                    feature_no,
                    genome_version_no,
                    seq_type="Protein",
                    residues=protein_seq,
                    dry_run=dry_run
                )
                if seq_no:
                    sequences_created += 1

        if (i + 1) % 500 == 0:
            logger.info(f"Processed {i + 1}/{len(genes)} genes...")
            if not dry_run:
                session.commit()

    if not dry_run:
        session.commit()

    logger.info(f"Created {features_created} features, {sequences_created} sequences")


def main():
    parser = argparse.ArgumentParser(description="Load C. tropicalis genes and sequences")
    parser.add_argument("--gff", required=True, type=Path, help="GFF file with gene annotations")
    parser.add_argument("--proteins", required=True, type=Path, help="Protein FASTA file")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Loading C. tropicalis genes and sequences")
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("[DRY RUN MODE]")

    with SessionLocal() as session:
        load_genes_and_proteins(session, args.gff, args.proteins, args.dry_run)

    logger.info("Done!")


if __name__ == "__main__":
    main()
