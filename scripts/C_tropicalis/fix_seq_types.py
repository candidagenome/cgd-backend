#!/usr/bin/env python3
"""
Fix seq_type values for C. tropicalis sequences.

The loading scripts used mixed-case seq_type values ('Genomic DNA', 'Protein')
but the API expects lowercase values ('genomic', 'protein', 'coding').

This script updates the seq_type values to match the expected format.

Usage:
    python fix_seq_types.py [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
ORGANISM_NAME = "Candida tropicalis MYA-3404"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fix_seq_types(session, dry_run: bool = False):
    """Fix seq_type values for C. tropicalis sequences."""

    # Get organism_no
    query = text(f"SELECT organism_no FROM {DB_SCHEMA}.organism WHERE organism_name = :name")
    result = session.execute(query, {"name": ORGANISM_NAME}).first()
    if not result:
        logger.error(f"Organism not found: {ORGANISM_NAME}")
        return
    organism_no = result[0]
    logger.info(f"Found organism_no: {organism_no}")

    # Mappings: old value -> new value
    seq_type_mappings = {
        'Genomic DNA': 'genomic',
        'Protein': 'protein',
        'CDS': 'coding',
    }

    for old_type, new_type in seq_type_mappings.items():
        # Count affected records
        count_query = text(f"""
            SELECT COUNT(*) FROM {DB_SCHEMA}.seq s
            JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
            WHERE f.organism_no = :org_no
            AND s.seq_type = :old_type
        """)
        result = session.execute(count_query, {"org_no": organism_no, "old_type": old_type}).first()
        count = result[0] if result else 0

        if count == 0:
            logger.info(f"No sequences with seq_type='{old_type}' to update")
            continue

        logger.info(f"Found {count} sequences with seq_type='{old_type}' -> '{new_type}'")

        if dry_run:
            logger.info(f"[DRY RUN] Would update {count} sequences")
            continue

        # Update seq_type
        update_query = text(f"""
            UPDATE {DB_SCHEMA}.seq s
            SET s.seq_type = :new_type
            WHERE s.feature_no IN (
                SELECT f.feature_no FROM {DB_SCHEMA}.feature f
                WHERE f.organism_no = :org_no
            )
            AND s.seq_type = :old_type
        """)
        session.execute(update_query, {
            "org_no": organism_no,
            "old_type": old_type,
            "new_type": new_type,
        })
        logger.info(f"Updated {count} sequences: '{old_type}' -> '{new_type}'")

    if not dry_run:
        session.commit()
        logger.info("Changes committed")


def main():
    parser = argparse.ArgumentParser(description="Fix seq_type values for C. tropicalis")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Fixing C. tropicalis seq_type values")
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("[DRY RUN MODE]")

    with SessionLocal() as session:
        fix_seq_types(session, args.dry_run)

    logger.info("Done!")


if __name__ == "__main__":
    main()
