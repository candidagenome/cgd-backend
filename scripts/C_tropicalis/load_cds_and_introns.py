#!/usr/bin/env python3
"""
Load C. tropicalis CDS and intron features from GFF coordinates.

CDS and introns are stored as:
1. Feature records with feature_type='CDS' or 'intron'
2. Linked to parent gene via FEAT_RELATIONSHIP (relationship_type='part of', rank=2)
3. With their own FEAT_LOCATION entries (SEQ_NO is null, only ROOT_SEQ_NO)

Usage:
    python load_cds_and_introns.py --gff FILE [--dry-run]

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import defaultdict
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


def ensure_code(session, table_name: str, col_name: str, code_value: str,
                description: str, dry_run: bool = False) -> bool:
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


def parse_gff_for_cds(gff_file: Path) -> Tuple[Dict[str, Dict], Dict[str, List[Dict]], Dict[str, str]]:
    """
    Parse GFF file and extract gene info and CDS coordinates.

    Returns:
        Tuple of (gene_info dict, gene_to_cds dict, gene_to_protein mapping)
    """
    gene_info = {}
    gene_to_cds = defaultdict(list)
    gene_to_protein = {}
    mrna_to_gene = {}

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

            # Track mRNA -> gene mapping
            if feature_type == 'mRNA':
                mrna_id = attr_dict.get('ID', '')
                parent = attr_dict.get('Parent', '')
                if mrna_id and parent:
                    mrna_to_gene[mrna_id] = parent

            # Extract CDS features
            if feature_type == 'CDS':
                protein_id = attr_dict.get('protein_id', '')
                gene_id = attr_dict.get('gene_id', '')
                parent = attr_dict.get('Parent', '')

                if protein_id and gene_id:
                    gene_to_protein[gene_id] = protein_id

                # Get actual gene_id from parent chain
                actual_gene_id = gene_id or mrna_to_gene.get(parent, parent)

                # Store CDS coordinates
                if actual_gene_id:
                    gene_to_cds[actual_gene_id].append({
                        'start': int(start),
                        'end': int(end),
                        'strand': strand,
                        'chromosome': seqid,
                    })

            # Process gene features
            if feature_type == 'gene':
                gene_id = attr_dict.get('ID', '').split(',')[0]
                if gene_id:
                    gene_info[gene_id] = {
                        'chromosome': seqid,
                        'start': int(start),
                        'end': int(end),
                        'strand': strand,
                    }

    # Sort CDS by start coordinate for each gene
    for gene_id in gene_to_cds:
        gene_to_cds[gene_id].sort(key=lambda x: x['start'])

    return gene_info, dict(gene_to_cds), gene_to_protein


def calculate_introns(cds_list: List[Dict]) -> List[Dict]:
    """Calculate intron positions from sorted CDS list."""
    if len(cds_list) < 2:
        return []

    introns = []
    sorted_cds = sorted(cds_list, key=lambda x: x['start'])

    for i in range(len(sorted_cds) - 1):
        intron_start = sorted_cds[i]['end'] + 1
        intron_end = sorted_cds[i + 1]['start'] - 1

        if intron_start <= intron_end:
            introns.append({
                'start': intron_start,
                'end': intron_end,
                'strand': sorted_cds[i]['strand'],
                'chromosome': sorted_cds[i]['chromosome'],
            })

    return introns


def get_organism_no(session) -> int:
    """Get organism_no for C. tropicalis."""
    query = text(f"SELECT organism_no FROM {DB_SCHEMA}.organism WHERE organism_name = :name")
    result = session.execute(query, {"name": ORGANISM_NAME}).first()
    if not result:
        raise ValueError(f"Organism not found: {ORGANISM_NAME}")
    return result[0]


def get_feature_no(session, feature_name: str) -> Optional[int]:
    """Get feature_no by feature_name."""
    query = text(f"SELECT feature_no FROM {DB_SCHEMA}.feature WHERE feature_name = :name")
    result = session.execute(query, {"name": feature_name}).first()
    return result[0] if result else None


def get_root_seq_no(session, feature_no: int) -> Optional[int]:
    """Get root_seq_no from parent feature's feat_location."""
    query = text(f"""
        SELECT root_seq_no FROM {DB_SCHEMA}.feat_location
        WHERE feature_no = :fno AND is_loc_current = 'Y'
    """)
    result = session.execute(query, {"fno": feature_no}).first()
    return result[0] if result else None


def feature_exists(session, feature_name: str) -> bool:
    """Check if feature already exists."""
    query = text(f"SELECT feature_no FROM {DB_SCHEMA}.feature WHERE feature_name = :name")
    result = session.execute(query, {"name": feature_name}).first()
    return result is not None


def create_feature(session, organism_no: int, feature_name: str, feature_type: str) -> Optional[int]:
    """Create a CDS or intron feature."""
    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.feature (
            organism_no, feature_name, feature_type, source, created_by
        ) VALUES (
            :org_no, :name, :ftype, :source, :created_by
        )
    """)
    session.execute(insert, {
        "org_no": organism_no, "name": feature_name,
        "ftype": feature_type, "source": SOURCE, "created_by": ADMIN_USER,
    })
    return get_feature_no(session, feature_name)


def create_feat_relationship(session, parent_feature_no: int, child_feature_no: int) -> bool:
    """Create a feat_relationship linking child to parent."""
    # Check if exists
    query = text(f"""
        SELECT feat_relationship_no FROM {DB_SCHEMA}.feat_relationship
        WHERE parent_feature_no = :parent AND child_feature_no = :child
    """)
    result = session.execute(query, {"parent": parent_feature_no, "child": child_feature_no}).first()
    if result:
        return False

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.feat_relationship (
            parent_feature_no, child_feature_no, relationship_type, rank, created_by
        ) VALUES (
            :parent, :child, 'part of', 2, :created_by
        )
    """)
    session.execute(insert, {
        "parent": parent_feature_no, "child": child_feature_no, "created_by": ADMIN_USER,
    })
    return True


def create_feat_location(session, feature_no: int, root_seq_no: int,
                          start_coord: int, stop_coord: int, strand: str) -> bool:
    """Create a feat_location entry."""
    # Check if exists
    query = text(f"""
        SELECT feat_location_no FROM {DB_SCHEMA}.feat_location
        WHERE feature_no = :fno AND is_loc_current = 'Y'
    """)
    result = session.execute(query, {"fno": feature_no}).first()
    if result:
        return False

    # Convert strand format: + -> W, - -> C
    db_strand = 'W' if strand == '+' else 'C'

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.feat_location (
            feature_no, root_seq_no, coord_version, start_coord, stop_coord,
            strand, is_loc_current, created_by
        ) VALUES (
            :fno, :root_seq_no, SYSDATE, :start, :stop, :strand, 'Y', :created_by
        )
    """)
    session.execute(insert, {
        "fno": feature_no, "root_seq_no": root_seq_no,
        "start": start_coord, "stop": stop_coord,
        "strand": db_strand, "created_by": ADMIN_USER,
    })
    return True


def load_cds_and_introns(session, gff_file: Path, dry_run: bool = False):
    """Load CDS and intron features from GFF."""
    # Ensure required codes
    ensure_code(session, "FEATURE", "FEATURE_TYPE", "CDS", "Coding sequence feature", dry_run)
    ensure_code(session, "FEATURE", "FEATURE_TYPE", "intron", "Intron feature", dry_run)
    ensure_code(session, "FEAT_RELATIONSHIP", "RELATIONSHIP_TYPE", "part of",
                "Part of relationship", dry_run)

    organism_no = get_organism_no(session)
    logger.info(f"Loading CDS and introns for organism_no={organism_no}")

    # Parse GFF
    logger.info(f"Parsing GFF: {gff_file}")
    gene_info, gene_to_cds, gene_to_protein = parse_gff_for_cds(gff_file)

    genes_with_cds = len(gene_to_cds)
    total_cds = sum(len(cds_list) for cds_list in gene_to_cds.values())
    logger.info(f"Found {genes_with_cds} genes with {total_cds} CDS segments")

    cds_created = 0
    introns_created = 0
    genes_not_found = 0
    genes_processed = 0

    for gene_id, cds_list in gene_to_cds.items():
        genes_processed += 1

        # Find parent feature
        protein_id = gene_to_protein.get(gene_id)
        parent_feature_no = None
        parent_name = None

        if protein_id:
            parent_feature_no = get_feature_no(session, protein_id)
            parent_name = protein_id
        if not parent_feature_no:
            parent_feature_no = get_feature_no(session, gene_id)
            parent_name = gene_id

        if not parent_feature_no:
            genes_not_found += 1
            continue

        # Get root_seq_no from parent's location
        root_seq_no = get_root_seq_no(session, parent_feature_no)
        if not root_seq_no:
            continue

        # Sort CDS by start coordinate
        sorted_cds = sorted(cds_list, key=lambda x: x['start'])

        # Create CDS features
        for i, cds in enumerate(sorted_cds, 1):
            cds_name = f"{parent_name}_cds{i}"

            if feature_exists(session, cds_name):
                continue

            if dry_run:
                cds_created += 1
                continue

            cds_feature_no = create_feature(session, organism_no, cds_name, "CDS")
            if cds_feature_no:
                create_feat_relationship(session, parent_feature_no, cds_feature_no)
                create_feat_location(
                    session, cds_feature_no, root_seq_no,
                    cds['start'], cds['end'], cds['strand']
                )
                cds_created += 1

        # Calculate and create intron features
        introns = calculate_introns(cds_list)
        for i, intron in enumerate(introns, 1):
            intron_name = f"{parent_name}_intron{i}"

            if feature_exists(session, intron_name):
                continue

            if dry_run:
                introns_created += 1
                continue

            intron_feature_no = create_feature(session, organism_no, intron_name, "intron")
            if intron_feature_no:
                create_feat_relationship(session, parent_feature_no, intron_feature_no)
                create_feat_location(
                    session, intron_feature_no, root_seq_no,
                    intron['start'], intron['end'], intron['strand']
                )
                introns_created += 1

        if genes_processed % 500 == 0:
            logger.info(f"Processed {genes_processed}/{genes_with_cds} genes...")
            if not dry_run:
                session.commit()

    if not dry_run:
        session.commit()

    logger.info("=" * 60)
    logger.info(f"Genes processed: {genes_processed}")
    logger.info(f"Genes not found in database: {genes_not_found}")
    logger.info(f"CDS features created: {cds_created}")
    logger.info(f"Intron features created: {introns_created}")


def main():
    parser = argparse.ArgumentParser(description="Load C. tropicalis CDS and introns")
    parser.add_argument("--gff", required=True, type=Path, help="GFF file")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Loading C. tropicalis CDS and intron features")
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("[DRY RUN MODE]")

    with SessionLocal() as session:
        load_cds_and_introns(session, args.gff, args.dry_run)

    logger.info("Done!")


if __name__ == "__main__":
    main()
