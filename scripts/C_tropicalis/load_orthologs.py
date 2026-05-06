#!/usr/bin/env python3
"""
Load C. tropicalis orthologs (BLAST reciprocal best hits) into database.

This script loads ortholog relationships between C. tropicalis and other
Candida species from reciprocal best hit files.

Usage:
    python load_orthologs.py --orthologs-dir DIR [--dry-run]

Data files needed:
    - reciprocal_best_hits.txt (C. tropicalis vs C. albicans)
    - reciprocal_best_hits_caur.txt (C. tropicalis vs C. auris)
    - reciprocal_best_hits_cdub.txt (C. tropicalis vs C. dubliniensis)
    - reciprocal_best_hits_cgla.txt (C. tropicalis vs C. glabrata)
    - reciprocal_best_hits_cpar.txt (C. tropicalis vs C. parapsilosis)

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
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

# Ortholog files mapping
ORTHOLOG_FILES = {
    "reciprocal_best_hits.txt": "C. albicans SC5314",
    "reciprocal_best_hits_caur.txt": "C. auris B8441",
    "reciprocal_best_hits_cdub.txt": "C. dubliniensis CD36",
    "reciprocal_best_hits_cgla.txt": "C. glabrata CBS138",
    "reciprocal_best_hits_cpar.txt": "C. parapsilosis CDC317",
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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


def parse_ortholog_file(filepath: Path) -> List[Tuple[str, str]]:
    """Parse reciprocal best hits file."""
    orthologs = []
    with open(filepath, 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        header = next(reader)  # Skip header
        for row in reader:
            if len(row) >= 2:
                ctrop_id = row[0].strip()
                other_id = row[1].strip()
                orthologs.append((ctrop_id, other_id))
    return orthologs


def get_feature_no_by_name(session, feature_name: str) -> Optional[int]:
    """Get feature_no by feature_name."""
    query = text(f"""
        SELECT feature_no
        FROM {DB_SCHEMA}.feature
        WHERE feature_name = :feature_name
    """)
    result = session.execute(query, {"feature_name": feature_name}).first()
    return result[0] if result else None


def get_feature_no_by_dbxref(session, dbxref_pattern: str) -> Optional[int]:
    """Get feature_no by dbxref_id pattern."""
    query = text(f"""
        SELECT feature_no
        FROM {DB_SCHEMA}.feature
        WHERE dbxref_id LIKE :pattern
    """)
    result = session.execute(query, {"pattern": f"%{dbxref_pattern}%"}).first()
    return result[0] if result else None


def create_homology_group(
    session,
    homology_type: str = "ortholog",
    method: str = "BLAST RBH",
    dry_run: bool = False
) -> Optional[int]:
    """Create a homology group."""
    if dry_run:
        return None

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.homology_group (
            homology_group_type, method, created_by
        ) VALUES (
            :homology_type, :method, :created_by
        )
    """)
    session.execute(insert, {
        "homology_type": homology_type,
        "method": method,
        "created_by": ADMIN_USER,
    })

    # Get the last inserted homology_group_no
    query = text(f"""
        SELECT MAX(homology_group_no)
        FROM {DB_SCHEMA}.homology_group
    """)
    result = session.execute(query).first()
    return result[0] if result else None


def add_feature_to_homology_group(
    session,
    feature_no: int,
    homology_group_no: int,
    dry_run: bool = False
) -> bool:
    """Add a feature to a homology group."""
    # Check if already exists
    query = text(f"""
        SELECT feat_homology_no
        FROM {DB_SCHEMA}.feat_homology
        WHERE feature_no = :feature_no
        AND homology_group_no = :homology_group_no
    """)
    result = session.execute(query, {
        "feature_no": feature_no,
        "homology_group_no": homology_group_no,
    }).first()

    if result:
        return False

    if dry_run:
        return True

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.feat_homology (
            feature_no, homology_group_no, created_by
        ) VALUES (
            :feature_no, :homology_group_no, :created_by
        )
    """)
    session.execute(insert, {
        "feature_no": feature_no,
        "homology_group_no": homology_group_no,
        "created_by": ADMIN_USER,
    })
    return True


def load_orthologs_from_file(
    session,
    filepath: Path,
    other_species: str,
    dry_run: bool = False
) -> Tuple[int, int, int]:
    """Load orthologs from a single file."""
    logger.info(f"Loading orthologs from {filepath.name} ({other_species})")

    orthologs = parse_ortholog_file(filepath)
    logger.info(f"Found {len(orthologs)} ortholog pairs")

    groups_created = 0
    pairs_loaded = 0
    pairs_skipped = 0

    for ctrop_id, other_id in orthologs:
        # Find C. tropicalis feature
        ctrop_feature_no = get_feature_no_by_name(session, ctrop_id)
        if not ctrop_feature_no:
            # Try by dbxref pattern (protein ID like EER30087.1)
            ctrop_feature_no = get_feature_no_by_dbxref(session, ctrop_id)

        if not ctrop_feature_no:
            pairs_skipped += 1
            continue

        # Find other species feature
        other_feature_no = get_feature_no_by_name(session, other_id)
        if not other_feature_no:
            other_feature_no = get_feature_no_by_dbxref(session, other_id)

        if not other_feature_no:
            pairs_skipped += 1
            continue

        # Create homology group and add both features
        if not dry_run:
            homology_group_no = create_homology_group(session, dry_run=dry_run)
            if homology_group_no:
                add_feature_to_homology_group(session, ctrop_feature_no, homology_group_no, dry_run)
                add_feature_to_homology_group(session, other_feature_no, homology_group_no, dry_run)
                groups_created += 1
                pairs_loaded += 1
        else:
            pairs_loaded += 1

    return groups_created, pairs_loaded, pairs_skipped


def main():
    parser = argparse.ArgumentParser(description="Load C. tropicalis orthologs")
    parser.add_argument("--orthologs-dir", required=True, type=Path, help="Directory with ortholog files")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Loading C. tropicalis orthologs")
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("[DRY RUN MODE]")

    total_groups = 0
    total_loaded = 0
    total_skipped = 0

    with SessionLocal() as session:
        # Ensure required codes exist
        ensure_code(session, "HOMOLOGY_GROUP", "METHOD", "BLAST RBH",
                    "BLAST Reciprocal Best Hit method for ortholog detection", args.dry_run)
        ensure_code(session, "HOMOLOGY_GROUP", "HOMOLOGY_GROUP_TYPE", "ortholog",
                    "Ortholog relationship type", args.dry_run)

        for filename, other_species in ORTHOLOG_FILES.items():
            filepath = args.orthologs_dir / filename
            if filepath.exists():
                groups, loaded, skipped = load_orthologs_from_file(
                    session, filepath, other_species, args.dry_run
                )
                total_groups += groups
                total_loaded += loaded
                total_skipped += skipped
                if not args.dry_run:
                    session.commit()
            else:
                logger.warning(f"File not found: {filepath}")

    logger.info("=" * 60)
    logger.info(f"Total homology groups created: {total_groups}")
    logger.info(f"Total ortholog pairs loaded: {total_loaded}")
    logger.info(f"Total pairs skipped (features not found): {total_skipped}")
    logger.info("Done!")


if __name__ == "__main__":
    main()
