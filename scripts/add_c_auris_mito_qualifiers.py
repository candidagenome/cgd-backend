#!/usr/bin/env python3
"""
Add 'Uncharacterized' feature qualifier to C. auris B8441 mitochondrial ORFs.

This script adds the 'Uncharacterized' feature_qualifier property to all
C. auris B8441 mitochondrial ORFs that don't already have a qualifier.

Usage:
    python scripts/add_c_auris_mito_qualifiers.py --dry-run  # Preview changes
    python scripts/add_c_auris_mito_qualifiers.py            # Apply changes

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

from dotenv import load_dotenv
from sqlalchemy import text

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
ADMIN_USER = os.getenv("ADMIN_USER", "cgdadmin").upper()

# C. auris B8441 mitochondrial configuration
ORGANISM_ABBREV = "C_auris_B8441"
MITO_CHROMOSOME = "MT849287.1_C_auris_B8441_mito"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_mito_orfs_without_qualifier(session) -> list:
    """
    Get C. auris B8441 mitochondrial ORFs that don't have a feature_qualifier.

    Returns list of tuples: (feature_no, feature_name, gene_name)
    """
    query = text(f"""
        SELECT DISTINCT f.feature_no, f.feature_name, f.gene_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
        JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
        JOIN {DB_SCHEMA}.feature chrom ON s.seq_no = chrom.feature_no
        WHERE chrom.feature_name = :mito_chromosome
        AND f.feature_type = 'ORF'
        AND fl.is_loc_current = 'Y'
        AND f.feature_no NOT IN (
            SELECT feature_no FROM {DB_SCHEMA}.feat_property
            WHERE property_type = 'feature_qualifier'
        )
        ORDER BY f.feature_name
    """)
    result = session.execute(
        query, {"mito_chromosome": MITO_CHROMOSOME}
    ).fetchall()
    return [(row[0], row[1], row[2]) for row in result]


def get_max_feat_property_no(session) -> int:
    """Get the maximum feat_property_no currently in use."""
    query = text(f"""
        SELECT MAX(feat_property_no)
        FROM {DB_SCHEMA}.feat_property
    """)
    result = session.execute(query).scalar()
    return result or 0


def add_uncharacterized_qualifiers(session, dry_run: bool = False):
    """Add 'Uncharacterized' qualifier to C. auris mito ORFs."""
    # Get ORFs that need qualifiers
    orfs = get_mito_orfs_without_qualifier(session)
    logger.info(f"Found {len(orfs)} mitochondrial ORFs without feature_qualifier")

    if not orfs:
        logger.info("No ORFs need qualifiers. Done!")
        return

    # List the ORFs
    logger.info("ORFs to update:")
    for feature_no, feature_name, gene_name in orfs:
        logger.info(f"  - {feature_name} ({gene_name or 'no gene name'})")

    if dry_run:
        logger.info(f"[DRY RUN] Would add 'Uncharacterized' qualifier to {len(orfs)} ORFs")
        return

    # Get starting feat_property_no
    max_prop_no = get_max_feat_property_no(session)
    next_prop_no = max_prop_no + 1
    logger.info(f"Starting feat_property_no: {next_prop_no}")

    # Insert qualifiers
    insert_query = text(f"""
        INSERT INTO {DB_SCHEMA}.feat_property (
            feat_property_no, feature_no, source, property_type,
            property_value, date_created, created_by
        ) VALUES (
            :feat_property_no, :feature_no, 'CGD', 'feature_qualifier',
            'Uncharacterized', SYSDATE, :created_by
        )
    """)

    for feature_no, feature_name, gene_name in orfs:
        session.execute(insert_query, {
            "feat_property_no": next_prop_no,
            "feature_no": feature_no,
            "created_by": ADMIN_USER,
        })
        logger.info(f"Added qualifier to {feature_name} (feat_property_no={next_prop_no})")
        next_prop_no += 1

    session.commit()
    logger.info(f"Successfully added 'Uncharacterized' qualifier to {len(orfs)} ORFs")


def main():
    parser = argparse.ArgumentParser(
        description="Add 'Uncharacterized' feature_qualifier to C. auris mito ORFs"
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Adding feature qualifiers to C. auris B8441 mitochondrial ORFs")
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("[DRY RUN MODE]")

    with SessionLocal() as session:
        add_uncharacterized_qualifiers(session, args.dry_run)

    logger.info("Done!")


if __name__ == "__main__":
    main()
