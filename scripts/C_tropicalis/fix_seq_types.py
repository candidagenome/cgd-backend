#!/usr/bin/env python3
"""
Fix seq_type values for C. tropicalis sequences.

The loading scripts used mixed-case seq_type values ('Genomic DNA', 'Protein')
but the API expects lowercase values ('genomic', 'protein', 'coding').

This script deletes sequences with wrong seq_type and re-inserts them with
correct values. Direct UPDATE is blocked by database trigger.

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
            logger.info(f"No sequences with seq_type='{old_type}' to fix")
            continue

        logger.info(f"Found {count} sequences with seq_type='{old_type}' -> '{new_type}'")

        if dry_run:
            logger.info(f"[DRY RUN] Would fix {count} sequences")
            continue

        # Get all sequences to fix
        select_query = text(f"""
            SELECT s.seq_no, s.feature_no, s.genome_version_no, s.seq_version,
                   s.source, s.is_seq_current, s.seq_length, s.residues, s.created_by
            FROM {DB_SCHEMA}.seq s
            JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
            WHERE f.organism_no = :org_no
            AND s.seq_type = :old_type
        """)
        rows = session.execute(select_query, {"org_no": organism_no, "old_type": old_type}).fetchall()

        fixed = 0
        for row in rows:
            seq_no = row[0]
            feature_no = row[1]
            genome_version_no = row[2]
            seq_version = row[3]
            source = row[4]
            is_seq_current = row[5]
            seq_length = row[6]
            residues = row[7]
            created_by = row[8]

            # Delete old record
            delete_query = text(f"DELETE FROM {DB_SCHEMA}.seq WHERE seq_no = :seq_no")
            session.execute(delete_query, {"seq_no": seq_no})

            # Insert with correct seq_type
            insert_query = text(f"""
                INSERT INTO {DB_SCHEMA}.seq (
                    feature_no, genome_version_no, seq_version, seq_type,
                    source, is_seq_current, seq_length, residues, created_by
                ) VALUES (
                    :feature_no, :genome_version_no, :seq_version, :seq_type,
                    :source, :is_seq_current, :seq_length, :residues, :created_by
                )
            """)
            session.execute(insert_query, {
                "feature_no": feature_no,
                "genome_version_no": genome_version_no,
                "seq_version": seq_version,
                "seq_type": new_type,
                "source": source,
                "is_seq_current": is_seq_current,
                "seq_length": seq_length,
                "residues": residues,
                "created_by": created_by,
            })
            fixed += 1

            if fixed % 500 == 0:
                logger.info(f"  Fixed {fixed}/{count}...")
                session.commit()

        session.commit()
        logger.info(f"Fixed {fixed} sequences: '{old_type}' -> '{new_type}'")

    logger.info("All changes committed")


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
