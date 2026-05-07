#!/usr/bin/env python3
"""
Update C. tropicalis features to use proper CAL-format CGD IDs.

Currently features have dbxref_id like "CTROP:CTRG_01181".
This script updates them to "CAL" format like "CAL0000300001".

Usage:
    python update_cgdids.py [--dry-run]

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

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
ADMIN_USER = os.getenv("ADMIN_USER", "cgdadmin").upper()

ORGANISM_NAME = "Candida tropicalis MYA-3404"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_max_cal_id(session) -> int:
    """Get the maximum CAL ID number currently in use."""
    query = text(f"""
        SELECT MAX(TO_NUMBER(SUBSTR(dbxref_id, 4)))
        FROM {DB_SCHEMA}.feature
        WHERE dbxref_id LIKE 'CAL%'
        AND REGEXP_LIKE(SUBSTR(dbxref_id, 4), '^[0-9]+$')
    """)
    result = session.execute(query).scalar()
    return result or 0


def get_ctrop_features(session) -> list:
    """Get all C. tropicalis features with CTROP: prefix."""
    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.dbxref_id
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
        WHERE o.organism_name = :org_name
        AND f.dbxref_id LIKE 'CTROP:%'
        ORDER BY f.feature_no
    """)
    result = session.execute(query, {"org_name": ORGANISM_NAME}).fetchall()
    return result


def update_dbxref_id(session, feature_no: int, new_dbxref_id: str) -> bool:
    """Update the dbxref_id for a feature."""
    update = text(f"""
        UPDATE {DB_SCHEMA}.feature
        SET dbxref_id = :new_id
        WHERE feature_no = :fno
    """)
    session.execute(update, {"new_id": new_dbxref_id, "fno": feature_no})
    return True


def update_cgdids(session, dry_run: bool = False):
    """Update all C. tropicalis features to use CAL IDs."""
    # Get current max CAL ID
    max_cal = get_max_cal_id(session)
    logger.info(f"Current max CAL ID: CAL{max_cal:010d}")

    # Get C. tropicalis features
    features = get_ctrop_features(session)
    logger.info(f"Found {len(features)} C. tropicalis features with CTROP: prefix")

    if not features:
        logger.info("No features to update")
        return

    # Update each feature
    next_cal = max_cal + 1
    updated = 0

    for feature_no, feature_name, old_dbxref_id in features:
        new_dbxref_id = f"CAL{next_cal:010d}"

        if dry_run:
            logger.info(f"[DRY RUN] {feature_name}: {old_dbxref_id} -> {new_dbxref_id}")
        else:
            update_dbxref_id(session, feature_no, new_dbxref_id)
            updated += 1

        next_cal += 1

        if updated % 500 == 0 and updated > 0:
            logger.info(f"Updated {updated} features...")
            if not dry_run:
                session.commit()

    if not dry_run:
        session.commit()

    logger.info("=" * 60)
    logger.info(f"Features processed: {len(features)}")
    logger.info(f"New CAL ID range: CAL{max_cal + 1:010d} to CAL{next_cal - 1:010d}")
    if dry_run:
        logger.info("[DRY RUN] No changes made")
    else:
        logger.info(f"Features updated: {updated}")


def main():
    parser = argparse.ArgumentParser(description="Update C. tropicalis CGD IDs to CAL format")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Updating C. tropicalis CGD IDs to CAL format")
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("[DRY RUN MODE]")

    with SessionLocal() as session:
        update_cgdids(session, args.dry_run)

    logger.info("Done!")


if __name__ == "__main__":
    main()
