#!/usr/bin/env python3
"""
Load Chibana chromosome 7 aliases into the database.

This script loads Chibana ORF name aliases from a mapping file into the
ALIAS, FEAT_ALIAS, and REF_LINK tables. The aliases are linked to a
reference via PubMed ID 15937140.

Input file format (tab-delimited):
- Column 1: Chibana ORF name (the alias to load)
- Column 2: CGD ORF name (existing feature name)

Original Perl: loadChibanaAliases.pl
Author: Prachi Shah
Converted to Python: 2024
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Alias, FeatAlias, Feature, RefLink, Reference

load_dotenv()

logger = logging.getLogger(__name__)

# Default PubMed ID for Chibana et al. reference
DEFAULT_PUBMED = "15937140"


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

    logger.info(f"Created alias: {alias_name}")
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
        return None

    new_entry = FeatAlias(
        feature_no=feature_no,
        alias_no=alias_no,
    )
    session.add(new_entry)
    session.flush()
    return new_entry.feat_alias_no


def create_ref_link_if_not_exists(
    session: Session,
    reference_no: int,
    alias_no: int,
    feature_no: int,
    created_by: str,
) -> bool:
    """
    Create REF_LINK entry for FEAT_ALIAS if it doesn't exist.

    Args:
        session: Database session
        reference_no: Reference number
        alias_no: Alias number
        feature_no: Feature number
        created_by: User creating the record

    Returns:
        True if created, False if already existed
    """
    # Primary key format for FEAT_ALIAS: alias_no::feature_no
    primary_key = f"{alias_no}::{feature_no}"

    existing = session.query(RefLink).filter(
        and_(
            RefLink.reference_no == reference_no,
            RefLink.tab_name == "FEAT_ALIAS",
            RefLink.primary_key == primary_key,
        )
    ).first()

    if existing:
        return False

    new_entry = RefLink(
        reference_no=reference_no,
        tab_name="FEAT_ALIAS",
        col_name="ALIAS_NO::FEATURE_NO",
        primary_key=primary_key,
        created_by=created_by[:12],
    )
    session.add(new_entry)
    return True


def parse_mapping_file(filepath: Path) -> list[dict]:
    """
    Parse Chibana ORF mapping file.

    Args:
        filepath: Path to mapping file

    Returns:
        List of dictionaries with chibana_name and orf
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
            if len(parts) < 2:
                continue

            chibana_name = parts[0].strip()
            orf = parts[1].strip()

            if chibana_name and orf:
                entries.append({
                    "chibana_name": chibana_name,
                    "orf": orf,
                })

    logger.info(f"Parsed {len(entries)} mappings from input file")
    return entries


def load_chibana_aliases(
    session: Session,
    entries: list[dict],
    reference_no: int,
    created_by: str,
) -> dict:
    """
    Load Chibana aliases into the database.

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
        "duplicates_skipped": 0,
    }

    seen_features = {}

    for entry in entries:
        chibana_name = entry["chibana_name"]
        orf = entry["orf"]

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

        stats["features_processed"] += 1

        # Create/get alias
        alias_no = get_or_create_alias(
            session, chibana_name, "Non-uniform", created_by
        )
        stats["aliases_created"] += 1

        # Create feat_alias link
        feat_alias_no = create_feat_alias_if_not_exists(
            session, feature.feature_no, alias_no
        )
        if feat_alias_no:
            stats["feat_aliases_created"] += 1
            logger.info(f"Linked {chibana_name} to {feature.feature_name}")

        # Create ref_link
        if create_ref_link_if_not_exists(
            session, reference_no, alias_no, feature.feature_no, created_by
        ):
            stats["ref_links_created"] += 1

        seen_features[feature.feature_no] = chibana_name

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load Chibana ORF aliases from mapping file"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input mapping file (tab-delimited: Chibana_name, ORF)",
    )
    parser.add_argument(
        "--pubmed",
        default=DEFAULT_PUBMED,
        help=f"PubMed ID for reference link (default: {DEFAULT_PUBMED})",
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
    logger.info(f"PubMed ID: {args.pubmed}")
    logger.info(f"Created by: {args.created_by}")

    # Parse input file
    entries = parse_mapping_file(args.input_file)

    if not entries:
        logger.warning("No mappings found in input file")
        return

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")
        logger.info(f"Would process {len(entries)} mappings")
        return

    try:
        with SessionLocal() as session:
            # Look up reference by PubMed ID
            reference = session.query(Reference).filter(
                Reference.pubmed == args.pubmed
            ).first()

            if not reference:
                logger.error(
                    f"Reference not found for PubMed ID: {args.pubmed}"
                )
                sys.exit(1)

            logger.info(
                f"Found reference_no={reference.reference_no} for "
                f"PubMed {args.pubmed}"
            )

            stats = load_chibana_aliases(
                session,
                entries,
                reference.reference_no,
                args.created_by,
            )

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Features processed: {stats['features_processed']}")
            logger.info(f"  Aliases created: {stats['aliases_created']}")
            logger.info(f"  FEAT_ALIAS links created: {stats['feat_aliases_created']}")
            logger.info(f"  REF_LINK entries created: {stats['ref_links_created']}")
            if stats["features_not_found"] > 0:
                logger.warning(f"  Features not found: {stats['features_not_found']}")
            if stats["duplicates_skipped"] > 0:
                logger.info(f"  Duplicates skipped: {stats['duplicates_skipped']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading Chibana aliases: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
