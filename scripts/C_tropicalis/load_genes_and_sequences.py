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


def get_max_cal_id(session) -> int:
    """Get the maximum CAL ID number currently in use.

    Checks both feature.dbxref_id and dbxref.dbxref_id tables since
    triggers may create dbxref entries that don't have features yet.
    """
    # Check feature table
    query1 = text(f"""
        SELECT MAX(TO_NUMBER(SUBSTR(dbxref_id, 4)))
        FROM {DB_SCHEMA}.feature
        WHERE dbxref_id LIKE 'CAL%'
        AND REGEXP_LIKE(SUBSTR(dbxref_id, 4), '^[0-9]+$')
    """)
    max_feature = session.execute(query1).scalar() or 0

    # Check dbxref table
    query2 = text(f"""
        SELECT MAX(TO_NUMBER(SUBSTR(dbxref_id, 4)))
        FROM {DB_SCHEMA}.dbxref
        WHERE dbxref_id LIKE 'CAL%'
        AND REGEXP_LIKE(SUBSTR(dbxref_id, 4), '^[0-9]+$')
    """)
    max_dbxref = session.execute(query2).scalar() or 0

    return max(max_feature, max_dbxref)


def ensure_code(session, table_name: str, col_name: str, code_value: str, description: str, dry_run: bool = False) -> bool:
    """Ensure a code value exists in the code table."""
    query = text(f"""
        SELECT code_no
        FROM {DB_SCHEMA}.code
        WHERE tab_name = :tab_name
        AND col_name = :col_name
        AND code_value = :code_value
    """)
    result = session.execute(query, {
        "tab_name": table_name,
        "col_name": col_name,
        "code_value": code_value
    }).first()

    if result:
        logger.info(f"Code already exists for {table_name}.{col_name}: {code_value}")
        return True

    if dry_run:
        logger.info(f"[DRY RUN] Would create code entry for {table_name}.{col_name}: {code_value}")
        return True

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.code (
            tab_name, col_name, code_value, description, created_by
        ) VALUES (
            :tab_name, :col_name, :code_value, :description, :created_by
        )
    """)
    session.execute(insert, {
        "tab_name": table_name,
        "col_name": col_name,
        "code_value": code_value,
        "description": description,
        "created_by": ADMIN_USER,
    })
    session.commit()
    logger.info(f"Created code entry for {table_name}.{col_name}: {code_value}")
    return True


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


def parse_gff(gff_file: Path) -> Tuple[List[Dict], Dict[str, str]]:
    """Parse GFF file and extract gene features and protein ID mapping.

    Returns:
        Tuple of (genes list, gene_id to protein_id mapping)
    """
    genes = []
    gene_to_protein = {}  # gene_id -> protein_id mapping from CDS lines

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

            # Parse attributes
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

            # Only process gene features from Liftoff source (CTRG_* genes)
            # Skip AUGUSTUS predictions (g1, g2, etc.)
            if feature_type != 'gene':
                continue
            if source != 'Liftoff':
                continue

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

    return genes, gene_to_protein


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

    # Ensure seq_type code exists
    ensure_code(session, "SEQ", "SEQ_TYPE", "Protein", "Protein sequence type", dry_run)

    organism_no = get_organism_no(session)
    genome_version_no = get_genome_version_no(session, organism_no)

    logger.info(f"Loading genes for organism_no={organism_no}, genome_version_no={genome_version_no}")

    # Get current max CAL ID for generating new CGD IDs
    max_cal_id = get_max_cal_id(session)
    next_cal_id = max_cal_id + 1
    logger.info(f"Starting CAL ID: CAL{next_cal_id:010d}")

    # Parse input files
    logger.info(f"Parsing GFF file: {gff_file}")
    genes, gene_to_protein = parse_gff(gff_file)
    logger.info(f"Found {len(genes)} genes in GFF")
    logger.info(f"Found {len(gene_to_protein)} gene-to-protein mappings from CDS features")

    logger.info(f"Parsing protein FASTA: {proteins_file}")
    proteins = parse_fasta(proteins_file)
    logger.info(f"Found {len(proteins)} protein sequences")

    # Load genes and proteins
    features_created = 0
    sequences_created = 0

    for i, gene in enumerate(genes):
        gene_id = gene['gene_id']

        # Get protein_id if available (from CDS mapping)
        protein_id = gene_to_protein.get(gene_id)

        # Use gene_id (e.g., CTRG_01181) as the systematic name (feature_name)
        # This is consistent with other CGD organisms and matches external databases
        feature_name = gene_id

        # Generate CAL-format CGD ID
        dbxref_id = f"CAL{next_cal_id:010d}"
        next_cal_id += 1

        feature_no = create_feature(
            session,
            organism_no,
            feature_name=feature_name,
            dbxref_id=dbxref_id,
            feature_type="ORF",
            dry_run=dry_run
        )

        if feature_no and not dry_run:
            features_created += 1

            # Try to find matching protein sequence using protein_id
            protein_seq = None
            if protein_id:
                protein_seq = proteins.get(protein_id)
            if not protein_seq:
                # Fallback: try gene_id directly
                protein_seq = proteins.get(gene_id)

            if protein_seq:
                seq_no = create_sequence(
                    session,
                    feature_no,
                    genome_version_no,
                    seq_type="protein",
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
