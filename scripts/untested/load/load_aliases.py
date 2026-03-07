#!/usr/bin/env python3
"""
Load extra aliases from external sources (e.g., Entrez Gene).

This script checks for aliases not already in the database and loads them
into the ALIAS, FEAT_ALIAS, and REF_LINK tables.

Input file format (tab-delimited):
- Column 1: CGDID (database cross-reference ID)
- Column 2: Gene ID (e.g., Entrez Gene ID)
- Column 3: Comma-separated list of aliases

Original Perl: checkAndLoadExtraAliases.pl
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
from cgd.models.models import Alias, FeatAlias, Feature, RefLink

load_dotenv()

logger = logging.getLogger(__name__)


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


def get_feature_by_dbxref_id(session: Session, dbxref_id: str) -> Feature | None:
    """
    Get feature by database cross-reference ID (CGDID).

    Args:
        session: Database session
        dbxref_id: Database cross-reference ID

    Returns:
        Feature object or None
    """
    return session.query(Feature).filter(
        Feature.dbxref_id == dbxref_id
    ).first()


def get_existing_aliases(session: Session, feature_no: int) -> set[str]:
    """
    Get all existing aliases for a feature.

    Args:
        session: Database session
        feature_no: Feature number

    Returns:
        Set of existing alias names
    """
    aliases = set()

    # Get feature name and gene name
    feature = session.query(Feature).filter(
        Feature.feature_no == feature_no
    ).first()

    if feature:
        aliases.add(feature.feature_name)
        if feature.gene_name:
            aliases.add(feature.gene_name)

    # Get all linked aliases
    feat_aliases = session.query(FeatAlias).filter(
        FeatAlias.feature_no == feature_no
    ).all()

    for fa in feat_aliases:
        alias = session.query(Alias).filter(
            Alias.alias_no == fa.alias_no
        ).first()
        if alias:
            aliases.add(alias.alias_name)

    return aliases


def determine_alias_type(name: str) -> str:
    """
    Determine alias type based on naming pattern.

    Args:
        name: Alias name

    Returns:
        Alias type string
    """
    # Uniform aliases follow pattern: 3 letters followed by digits
    if re.match(r"^[a-zA-Z]{3}\d+$", name):
        return "Uniform"
    return "Non-uniform"


def get_or_create_alias(
    session: Session,
    alias_name: str,
    alias_type: str,
    created_by: str,
) -> int:
    """
    Get existing alias or create new one.

    Args:
        session: Database session
        alias_name: Alias name
        alias_type: Type of alias
        created_by: User creating the record

    Returns:
        alias_no
    """
    existing = session.query(Alias).filter(
        and_(
            Alias.alias_name == alias_name,
            Alias.alias_type == alias_type,
        )
    ).first()

    if existing:
        return existing.alias_no

    new_alias = Alias(
        alias_name=alias_name,
        alias_type=alias_type,
        created_by=created_by[:12],
    )
    session.add(new_alias)
    session.flush()

    logger.info(f"Created alias: {alias_name}, type: {alias_type}")
    return new_alias.alias_no


def create_feat_alias_if_not_exists(
    session: Session,
    feature_no: int,
    alias_no: int,
) -> int | None:
    """
    Create FEAT_ALIAS entry if it doesn't exist.

    Args:
        session: Database session
        feature_no: Feature number
        alias_no: Alias number

    Returns:
        feat_alias_no or None if already existed
    """
    existing = session.query(FeatAlias).filter(
        and_(
            FeatAlias.feature_no == feature_no,
            FeatAlias.alias_no == alias_no,
        )
    ).first()

    if existing:
        return existing.feat_alias_no

    new_entry = FeatAlias(
        feature_no=feature_no,
        alias_no=alias_no,
    )
    session.add(new_entry)
    session.flush()

    logger.info(f"Created FEAT_ALIAS: feature_no={feature_no}, alias_no={alias_no}")
    return new_entry.feat_alias_no


def create_ref_link_if_not_exists(
    session: Session,
    reference_no: int,
    feat_alias_no: int,
    created_by: str,
) -> bool:
    """
    Create REF_LINK entry if it doesn't exist.

    Args:
        session: Database session
        reference_no: Reference number
        feat_alias_no: Feature alias number
        created_by: User creating the record

    Returns:
        True if created, False if already existed
    """
    existing = session.query(RefLink).filter(
        and_(
            RefLink.reference_no == reference_no,
            RefLink.tab_name == "FEAT_ALIAS",
            RefLink.col_name == "FEAT_ALIAS_NO",
            RefLink.primary_key == feat_alias_no,
        )
    ).first()

    if existing:
        return False

    new_entry = RefLink(
        reference_no=reference_no,
        tab_name="FEAT_ALIAS",
        col_name="FEAT_ALIAS_NO",
        primary_key=feat_alias_no,
        created_by=created_by[:12],
    )
    session.add(new_entry)
    logger.info(f"Created REF_LINK for feat_alias_no={feat_alias_no}")
    return True


def parse_input_file(filepath: Path) -> list[dict]:
    """
    Parse the input file.

    Args:
        filepath: Path to input file

    Returns:
        List of dictionaries with cgdid and aliases
    """
    entries = []

    with open(filepath) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) < 3:
                continue

            cgdid = parts[0].strip()
            # gene_id = parts[1].strip()  # Not used in this script
            aliases_str = parts[2].strip() if len(parts) > 2 else ""

            if not aliases_str:
                continue

            # Parse comma-separated aliases
            aliases = [a.strip() for a in aliases_str.split(",") if a.strip()]

            if aliases:
                entries.append({
                    "cgdid": cgdid,
                    "aliases": aliases,
                })

    logger.info(f"Parsed {len(entries)} entries from input file")
    return entries


def load_aliases(
    session: Session,
    entries: list[dict],
    reference_no: int,
    created_by: str,
) -> dict:
    """
    Load aliases into the database.

    Args:
        session: Database session
        entries: List of entry dictionaries
        reference_no: Reference number for ref_link
        created_by: User creating the records

    Returns:
        Dictionary with statistics
    """
    stats = {
        "features_processed": 0,
        "aliases_created": 0,
        "feat_aliases_created": 0,
        "ref_links_created": 0,
        "features_not_found": 0,
        "aliases_skipped": 0,
    }

    for entry in entries:
        cgdid = entry["cgdid"]
        aliases = entry["aliases"]

        # Find feature by CGDID
        feature = get_feature_by_dbxref_id(session, cgdid)
        if not feature:
            logger.warning(f"Cannot find feature for CGDID: {cgdid}")
            stats["features_not_found"] += 1
            continue

        stats["features_processed"] += 1

        # Get existing aliases
        existing_aliases = get_existing_aliases(session, feature.feature_no)

        for alias_name in aliases:
            # Skip if already exists
            if alias_name in existing_aliases:
                stats["aliases_skipped"] += 1
                continue

            # Determine alias type
            alias_type = determine_alias_type(alias_name)

            logger.info(f"{feature.feature_name}\t{alias_name}")

            # Create/get alias
            alias_no = get_or_create_alias(
                session, alias_name, alias_type, created_by
            )
            stats["aliases_created"] += 1

            # Create feat_alias
            feat_alias_no = create_feat_alias_if_not_exists(
                session, feature.feature_no, alias_no
            )
            if feat_alias_no:
                stats["feat_aliases_created"] += 1

                # Create ref_link
                if create_ref_link_if_not_exists(
                    session, reference_no, feat_alias_no, created_by
                ):
                    stats["ref_links_created"] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load extra aliases from external sources"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input file (tab-delimited: CGDID, GENEID, aliases)",
    )
    parser.add_argument(
        "reference_no",
        type=int,
        help="Reference number for ref_link associations",
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
    logger.info(f"Reference number: {args.reference_no}")
    logger.info(f"Created by: {args.created_by}")

    # Parse input file
    entries = parse_input_file(args.input_file)

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")
        total_aliases = sum(len(e["aliases"]) for e in entries)
        logger.info(f"Would process {len(entries)} features with {total_aliases} aliases")
        return

    try:
        with SessionLocal() as session:
            stats = load_aliases(
                session,
                entries,
                args.reference_no,
                args.created_by,
            )

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Features processed: {stats['features_processed']}")
            logger.info(f"  Aliases created: {stats['aliases_created']}")
            logger.info(f"  FEAT_ALIAS created: {stats['feat_aliases_created']}")
            logger.info(f"  REF_LINK created: {stats['ref_links_created']}")
            logger.info(f"  Aliases skipped (existing): {stats['aliases_skipped']}")
            if stats["features_not_found"] > 0:
                logger.warning(f"  Features not found: {stats['features_not_found']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading aliases: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
