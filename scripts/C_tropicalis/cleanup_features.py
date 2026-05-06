#!/usr/bin/env python3
"""
Cleanup script to delete C. tropicalis features before reloading.

Usage:
    python cleanup_features.py [--dry-run]
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


def main():
    parser = argparse.ArgumentParser(description="Cleanup C. tropicalis features")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    args = parser.parse_args()

    with SessionLocal() as session:
        # Get organism_no
        query = text(f"""
            SELECT organism_no FROM {DB_SCHEMA}.organism
            WHERE organism_name = :organism_name
        """)
        result = session.execute(query, {"organism_name": ORGANISM_NAME}).first()
        if not result:
            logger.error(f"Organism not found: {ORGANISM_NAME}")
            return
        organism_no = result[0]

        # Count features
        query = text(f"""
            SELECT COUNT(*) FROM {DB_SCHEMA}.feature WHERE organism_no = :organism_no
        """)
        count = session.execute(query, {"organism_no": organism_no}).first()[0]
        logger.info(f"Found {count} features for organism_no={organism_no}")

        if args.dry_run:
            logger.info("[DRY RUN] Would delete these features")
            return

        # Delete features
        delete = text(f"""
            DELETE FROM {DB_SCHEMA}.feature WHERE organism_no = :organism_no
        """)
        session.execute(delete, {"organism_no": organism_no})
        session.commit()
        logger.info(f"Deleted {count} features")


if __name__ == "__main__":
    main()
