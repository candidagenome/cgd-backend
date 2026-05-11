#!/usr/bin/env python3
"""
Add feature qualifiers (Uncharacterized) to C. tropicalis ORFs.

This script adds the 'Uncharacterized' feature_qualifier property to all
C. tropicalis ORFs that don't already have a qualifier. This is needed for
the Genome Snapshot page to display the ORF distribution pie chart correctly.

Usage:
    python add_feature_qualifiers.py [--dry-run]

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_organism_no(session) -> int:
    """Get organism_no for C. tropicalis."""
    query = text(f"""
        SELECT organism_no
        FROM {DB_SCHEMA}.organism
        WHERE common_name = :common_name
    """)
    result = session.execute(query, {"common_name": ORGANISM_NAME}).first()
    if not result:
        raise ValueError(f"Organism not found: {ORGANISM_NAME}")
    return result[0]


def get_current_orfs_without_qualifier(session, organism_no: int) -> list:
    """
    Get feature_nos for current ORFs that don't have a feature_qualifier.

    Filters for:
    - Current location (is_loc_current = 'Y')
    - Current sequence (is_seq_current = 'Y')
    - Current genome version (is_ver_current = 'Y')
    - Not deleted (no 'Deleted%' property)
    - No existing feature_qualifier property
    """
    query = text(f"""
        SELECT DISTINCT f.feature_no
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
        JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
        JOIN {DB_SCHEMA}.genome_version gv ON s.genome_version_no = gv.genome_version_no
        WHERE f.organism_no = :organism_no
        AND f.feature_type = 'ORF'
        AND fl.is_loc_current = 'Y'
        AND s.is_seq_current = 'Y'
        AND gv.is_ver_current = 'Y'
        AND f.feature_no NOT IN (
            SELECT feature_no FROM {DB_SCHEMA}.feat_property
            WHERE property_value LIKE 'Deleted%'
        )
        AND f.feature_no NOT IN (
            SELECT feature_no FROM {DB_SCHEMA}.feat_property
            WHERE property_type = 'feature_qualifier'
        )
    """)
    result = session.execute(query, {"organism_no": organism_no}).fetchall()
    return [row[0] for row in result]


def get_max_feat_property_no(session) -> int:
    """Get the maximum feat_property_no currently in use."""
    query = text(f"""
        SELECT MAX(feat_property_no)
        FROM {DB_SCHEMA}.feat_property
    """)
    result = session.execute(query).scalar()
    return result or 0


def add_uncharacterized_qualifiers(session, dry_run: bool = False):
    """Add 'Uncharacterized' qualifier to all C. tropicalis ORFs without one."""
    organism_no = get_organism_no(session)
    logger.info(f"Organism: {ORGANISM_NAME} (organism_no={organism_no})")

    # Get ORFs that need qualifiers
    feature_nos = get_current_orfs_without_qualifier(session, organism_no)
    logger.info(f"Found {len(feature_nos)} ORFs without feature_qualifier")

    if not feature_nos:
        logger.info("No ORFs need qualifiers. Done!")
        return

    if dry_run:
        logger.info(f"[DRY RUN] Would add 'Uncharacterized' qualifier to {len(feature_nos)} ORFs")
        return

    # Get starting feat_property_no
    max_prop_no = get_max_feat_property_no(session)
    next_prop_no = max_prop_no + 1
    logger.info(f"Starting feat_property_no: {next_prop_no}")

    # Insert qualifiers in batches
    batch_size = 500
    total_inserted = 0

    insert_query = text(f"""
        INSERT INTO {DB_SCHEMA}.feat_property (
            feat_property_no, feature_no, source, property_type,
            property_value, date_created, created_by
        ) VALUES (
            :feat_property_no, :feature_no, 'CGD', 'feature_qualifier',
            'Uncharacterized', SYSDATE, :created_by
        )
    """)

    for i in range(0, len(feature_nos), batch_size):
        batch = feature_nos[i:i + batch_size]

        for feature_no in batch:
            session.execute(insert_query, {
                "feat_property_no": next_prop_no,
                "feature_no": feature_no,
                "created_by": ADMIN_USER,
            })
            next_prop_no += 1
            total_inserted += 1

        session.commit()
        logger.info(f"Inserted batch {i // batch_size + 1} ({len(batch)} records)")

    logger.info(f"Successfully added 'Uncharacterized' qualifier to {total_inserted} ORFs")


def main():
    parser = argparse.ArgumentParser(
        description="Add 'Uncharacterized' feature_qualifier to C. tropicalis ORFs"
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Adding feature qualifiers to C. tropicalis ORFs")
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("[DRY RUN MODE]")

    with SessionLocal() as session:
        add_uncharacterized_qualifiers(session, args.dry_run)

    logger.info("Done!")


if __name__ == "__main__":
    main()
