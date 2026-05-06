#!/usr/bin/env python3
"""
Load C. tropicalis organism and genome version into database.

This script creates the organism entry and genome version for C. tropicalis MYA-3404.

Usage:
    python load_organism.py [--dry-run]

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
ORGANISM_CONFIG = {
    "common_name": "Candida tropicalis MYA-3404",
    "genus": "Candida",
    "species": "tropicalis",
    "strain_name": "MYA-3404",
    "taxon_id": 294747,
    "abbreviation": "C. tropicalis",
}

GENOME_VERSION_CONFIG = {
    "genome_version": "MYA-3404_v1",
    "description": "C. tropicalis MYA-3404 genome from NCBI (GCA_013177555.1)",
    "is_ver_current": "Y",
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_or_create_organism(session, dry_run: bool = False) -> int:
    """Get or create organism entry for C. tropicalis."""
    # Check if organism exists
    query = text(f"""
        SELECT organism_no
        FROM {DB_SCHEMA}.organism
        WHERE common_name = :common_name
    """)
    result = session.execute(query, {"common_name": ORGANISM_CONFIG["common_name"]}).first()

    if result:
        logger.info(f"Organism already exists: {ORGANISM_CONFIG['common_name']} (organism_no={result[0]})")
        return result[0]

    if dry_run:
        logger.info(f"[DRY RUN] Would create organism: {ORGANISM_CONFIG['common_name']}")
        return -1

    # Insert organism
    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.organism (
            common_name, genus, species, strain_name, taxon_id, abbreviation, created_by
        ) VALUES (
            :common_name, :genus, :species, :strain_name, :taxon_id, :abbreviation, :created_by
        )
    """)
    session.execute(insert, {
        **ORGANISM_CONFIG,
        "created_by": ADMIN_USER,
    })
    session.commit()

    # Get the new organism_no
    result = session.execute(query, {"common_name": ORGANISM_CONFIG["common_name"]}).first()
    organism_no = result[0]
    logger.info(f"Created organism: {ORGANISM_CONFIG['common_name']} (organism_no={organism_no})")
    return organism_no


def get_or_create_genome_version(session, organism_no: int, dry_run: bool = False) -> int:
    """Get or create genome version entry."""
    # Check if genome version exists
    query = text(f"""
        SELECT genome_version_no
        FROM {DB_SCHEMA}.genome_version
        WHERE genome_version = :genome_version
        AND organism_no = :organism_no
    """)
    result = session.execute(query, {
        "genome_version": GENOME_VERSION_CONFIG["genome_version"],
        "organism_no": organism_no,
    }).first()

    if result:
        logger.info(f"Genome version already exists: {GENOME_VERSION_CONFIG['genome_version']} (genome_version_no={result[0]})")
        return result[0]

    if dry_run:
        logger.info(f"[DRY RUN] Would create genome version: {GENOME_VERSION_CONFIG['genome_version']}")
        return -1

    # Insert genome version
    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.genome_version (
            genome_version, organism_no, is_ver_current, description, created_by
        ) VALUES (
            :genome_version, :organism_no, :is_ver_current, :description, :created_by
        )
    """)
    session.execute(insert, {
        **GENOME_VERSION_CONFIG,
        "organism_no": organism_no,
        "created_by": ADMIN_USER,
    })
    session.commit()

    # Get the new genome_version_no
    result = session.execute(query, {
        "genome_version": GENOME_VERSION_CONFIG["genome_version"],
        "organism_no": organism_no,
    }).first()
    genome_version_no = result[0]
    logger.info(f"Created genome version: {GENOME_VERSION_CONFIG['genome_version']} (genome_version_no={genome_version_no})")
    return genome_version_no


def main():
    parser = argparse.ArgumentParser(description="Load C. tropicalis organism into database")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Loading C. tropicalis organism and genome version")
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("[DRY RUN MODE - No changes will be made]")

    with SessionLocal() as session:
        organism_no = get_or_create_organism(session, args.dry_run)
        if organism_no > 0:
            genome_version_no = get_or_create_genome_version(session, organism_no, args.dry_run)

    logger.info("Done!")


if __name__ == "__main__":
    main()
