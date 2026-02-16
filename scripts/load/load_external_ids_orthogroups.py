#!/usr/bin/env python3
"""
Load external IDs for fungal orthogroup associations.

This script loads external identifiers from an orthogroup mapping file
and creates associations between features and orthogroup IDs.

The script creates entries in the DBXREF and DBXREF_FEAT tables to
link features to their orthogroup identifiers.

Input file format (tab-delimited, with header):
- Column 1: ORF/feature name
- Column 3: Orthogroup URL or ID

Original Perl: loadExternalIds_Orthogroups.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Alias, Dbxref, DbxrefFeat, FeatAlias, Feature

load_dotenv()

logger = logging.getLogger(__name__)

# Source and type for orthogroup dbxrefs
DBXREF_SOURCE = "Fungal Orthologs"
DBXREF_TYPE = "Orthogroup ID"


def setup_logging(log_file: Path = None, verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    handlers = [logging.StreamHandler()]

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def find_feature_by_name(session: Session, name: str) -> Feature | None:
    """
    Find feature by feature_name or alias.

    Args:
        session: Database session
        name: Feature name or alias to search

    Returns:
        Feature object or None
    """
    # Try by feature_name first
    feature = session.query(Feature).filter(
        Feature.feature_name == name
    ).first()

    if feature:
        return feature

    # Try by alias
    feat_alias = session.query(FeatAlias).join(
        Alias, FeatAlias.alias_no == Alias.alias_no
    ).filter(
        Alias.alias_name == name
    ).first()

    if feat_alias:
        return session.query(Feature).filter(
            Feature.feature_no == feat_alias.feature_no
        ).first()

    return None


def extract_orthogroup_id(url_or_id: str) -> str:
    """
    Extract orthogroup ID from URL or return ID directly.

    Args:
        url_or_id: URL like "http://.../.../ORTHO123.html" or just "ORTHO123"

    Returns:
        Extracted orthogroup ID
    """
    # Remove URL prefix if present
    ortho_id = re.sub(
        r"http://www\.broad\.mit\.edu/regev/orthogroups/html/",
        "",
        url_or_id
    )
    # Remove .html suffix
    ortho_id = re.sub(r"\.html$", "", ortho_id)
    return ortho_id.strip()


def get_or_create_dbxref(
    session: Session,
    dbxref_id: str,
    source: str,
    dbxref_type: str,
    created_by: str,
) -> int:
    """
    Get existing dbxref or create new one.

    Args:
        session: Database session
        dbxref_id: External identifier
        source: Source database
        dbxref_type: Type of cross-reference
        created_by: User creating the record

    Returns:
        dbxref_no
    """
    existing = session.query(Dbxref).filter(
        and_(
            Dbxref.dbxref_id == dbxref_id,
            Dbxref.source == source,
            Dbxref.dbxref_type == dbxref_type,
        )
    ).first()

    if existing:
        return existing.dbxref_no

    new_dbxref = Dbxref(
        dbxref_id=dbxref_id,
        source=source,
        dbxref_type=dbxref_type,
        created_by=created_by[:12],
    )
    session.add(new_dbxref)
    session.flush()

    logger.debug(f"Created dbxref: {dbxref_id}")
    return new_dbxref.dbxref_no


def create_dbxref_feat_if_not_exists(
    session: Session,
    feature_no: int,
    dbxref_no: int,
) -> bool:
    """
    Create DBXREF_FEAT entry if it doesn't exist.

    Args:
        session: Database session
        feature_no: Feature number
        dbxref_no: Dbxref number

    Returns:
        True if created, False if already existed
    """
    existing = session.query(DbxrefFeat).filter(
        and_(
            DbxrefFeat.feature_no == feature_no,
            DbxrefFeat.dbxref_no == dbxref_no,
        )
    ).first()

    if existing:
        return False

    new_entry = DbxrefFeat(
        feature_no=feature_no,
        dbxref_no=dbxref_no,
    )
    session.add(new_entry)
    return True


def parse_orthogroup_file(filepath: Path) -> list[dict]:
    """
    Parse orthogroup mapping file.

    Args:
        filepath: Path to mapping file

    Returns:
        List of dictionaries with orf and orthogroup_id
    """
    entries = []

    with open(filepath) as f:
        for line_num, line in enumerate(f, 1):
            # Skip header
            if line_num == 1:
                continue

            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) < 3:
                continue

            orf = parts[0].strip()
            url_or_id = parts[2].strip() if len(parts) > 2 else ""

            if not url_or_id:
                continue

            orthogroup_id = extract_orthogroup_id(url_or_id)

            if orf and orthogroup_id:
                entries.append({
                    "orf": orf,
                    "orthogroup_id": orthogroup_id,
                })

    logger.info(f"Parsed {len(entries)} entries from input file")
    return entries


def load_orthogroup_ids(
    session: Session,
    entries: list[dict],
    created_by: str,
) -> dict:
    """
    Load orthogroup external IDs into the database.

    Args:
        session: Database session
        entries: List of entry dictionaries
        created_by: User creating the records

    Returns:
        Dictionary with statistics
    """
    stats = {
        "entries_processed": 0,
        "dbxrefs_created": 0,
        "dbxref_feats_created": 0,
        "features_not_found": 0,
        "duplicates_skipped": 0,
    }

    seen_features = {}

    for entry in entries:
        orf = entry["orf"]
        orthogroup_id = entry["orthogroup_id"]

        feature = find_feature_by_name(session, orf)

        if not feature:
            logger.warning(f"Cannot find feature for: {orf}")
            stats["features_not_found"] += 1
            continue

        # Skip if we've already processed this feature
        if feature.feature_no in seen_features:
            logger.debug(
                f"Feature {feature.feature_no} already processed as "
                f"{seen_features[feature.feature_no]}, skipping {orf}"
            )
            stats["duplicates_skipped"] += 1
            continue

        stats["entries_processed"] += 1

        # Create/get dbxref
        dbxref_no = get_or_create_dbxref(
            session,
            orthogroup_id,
            DBXREF_SOURCE,
            DBXREF_TYPE,
            created_by,
        )
        stats["dbxrefs_created"] += 1

        # Create dbxref_feat link
        if create_dbxref_feat_if_not_exists(
            session, feature.feature_no, dbxref_no
        ):
            stats["dbxref_feats_created"] += 1
            logger.info(
                f"Linked {orf} to orthogroup {orthogroup_id}"
            )

        seen_features[feature.feature_no] = orf

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load external IDs for fungal orthogroup associations"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input orthogroup mapping file (tab-delimited)",
    )
    parser.add_argument(
        "--created-by",
        default=os.getenv("DB_USER", "SCRIPT"),
        help="Database user name for created_by field",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Path to log file",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse file but don't modify database",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_file, args.verbose)

    # Validate input file
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    logger.info(f"Input file: {args.input_file}")
    logger.info(f"Created by: {args.created_by}")

    # Parse input file
    entries = parse_orthogroup_file(args.input_file)

    if not entries:
        logger.warning("No entries found in input file")
        return

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")
        logger.info(f"Would process {len(entries)} entries")
        return

    try:
        with SessionLocal() as session:
            stats = load_orthogroup_ids(session, entries, args.created_by)

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Entries processed: {stats['entries_processed']}")
            logger.info(f"  DBXREFs created: {stats['dbxrefs_created']}")
            logger.info(f"  DBXREF_FEAT links created: {stats['dbxref_feats_created']}")
            if stats["features_not_found"] > 0:
                logger.warning(f"  Features not found: {stats['features_not_found']}")
            if stats["duplicates_skipped"] > 0:
                logger.info(f"  Duplicates skipped: {stats['duplicates_skipped']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading orthogroup IDs: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
