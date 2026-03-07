#!/usr/bin/env python3
"""
Load IPF (Induced Protein Fragment) aliases into the database.

This script loads IPF aliases from annotation data files into the
ALIAS and FEAT_ALIAS tables.

Input file format (tab-delimited):
- Column 1: ORF name (feature identifier)
- Column 9: Allele information (may contain IPF identifiers)
- Column 13: IDs field (may contain IPF: prefixed identifiers)

The script extracts IPF identifiers from both the allele and IDs columns
and creates alias entries for them.

Original Perl: loadIPFAliases.pl
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
from cgd.models.models import Alias, FeatAlias, Feature

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
) -> bool:
    """
    Create FEAT_ALIAS entry if it doesn't exist.

    Args:
        session: Database session
        feature_no: Feature number
        alias_no: Alias number

    Returns:
        True if created, False if already existed
    """
    existing = session.query(FeatAlias).filter(
        and_(
            FeatAlias.feature_no == feature_no,
            FeatAlias.alias_no == alias_no,
        )
    ).first()

    if existing:
        return False

    new_entry = FeatAlias(
        feature_no=feature_no,
        alias_no=alias_no,
    )
    session.add(new_entry)
    return True


def extract_ipf_identifiers(allele_info: str, ids_field: str) -> list[str]:
    """
    Extract IPF identifiers from allele and IDs fields.

    Args:
        allele_info: Allele information field (pipe-delimited)
        ids_field: IDs field (pipe-delimited)

    Returns:
        List of unique IPF identifiers
    """
    ipfs = set()

    # Extract from allele info (e.g., "IPF1234" or "IPF1234.5")
    if allele_info:
        for part in allele_info.split("|"):
            match = re.search(r"(IPF[\d\.]+)", part)
            if match:
                ipfs.add(match.group(1))

    # Extract from IDs field (e.g., "IPF:1234.5")
    if ids_field:
        for part in ids_field.split("|"):
            match = re.search(r"IPF:([\d\.]+)", part)
            if match:
                ipfs.add("IPF" + match.group(1))

    return list(ipfs)


def parse_annotation_file(filepath: Path) -> list[dict]:
    """
    Parse annotation data file.

    Args:
        filepath: Path to annotation file

    Returns:
        List of dictionaries with orf and ipf_ids
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
            if len(parts) < 13:
                continue

            orf = parts[0].strip()
            allele_info = parts[8].strip() if len(parts) > 8 else ""
            ids_field = parts[12].strip() if len(parts) > 12 else ""

            ipf_ids = extract_ipf_identifiers(allele_info, ids_field)

            if ipf_ids:
                entries.append({
                    "orf": orf,
                    "ipf_ids": ipf_ids,
                })

    logger.info(f"Parsed {len(entries)} entries with IPF identifiers")
    return entries


def load_ipf_aliases(
    session: Session,
    entries: list[dict],
    created_by: str,
) -> dict:
    """
    Load IPF aliases into the database.

    Args:
        session: Database session
        entries: List of entry dictionaries
        created_by: User creating the records

    Returns:
        Dictionary with statistics
    """
    stats = {
        "features_processed": 0,
        "aliases_created": 0,
        "feat_aliases_created": 0,
        "features_not_found": 0,
        "duplicates_skipped": 0,
    }

    seen_features = {}

    for entry in entries:
        orf = entry["orf"]
        ipf_ids = entry["ipf_ids"]

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
        seen_features[feature.feature_no] = orf

        for ipf_id in ipf_ids:
            # Create/get alias
            alias_no = get_or_create_alias(
                session, ipf_id, "Non-uniform", created_by
            )
            stats["aliases_created"] += 1

            # Create feat_alias link
            if create_feat_alias_if_not_exists(
                session, feature.feature_no, alias_no
            ):
                stats["feat_aliases_created"] += 1
                logger.info(
                    f"Linked {ipf_id} to {feature.feature_name}"
                )

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load IPF aliases from annotation data"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input annotation file (tab-delimited)",
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
    entries = parse_annotation_file(args.input_file)

    if not entries:
        logger.warning("No entries with IPF identifiers found")
        return

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")
        total_ipfs = sum(len(e["ipf_ids"]) for e in entries)
        logger.info(
            f"Would process {len(entries)} features with {total_ipfs} IPF aliases"
        )
        return

    try:
        with SessionLocal() as session:
            stats = load_ipf_aliases(session, entries, args.created_by)

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Features processed: {stats['features_processed']}")
            logger.info(f"  Aliases created: {stats['aliases_created']}")
            logger.info(f"  FEAT_ALIAS links created: {stats['feat_aliases_created']}")
            if stats["features_not_found"] > 0:
                logger.warning(f"  Features not found: {stats['features_not_found']}")
            if stats["duplicates_skipped"] > 0:
                logger.info(f"  Duplicates skipped: {stats['duplicates_skipped']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading IPF aliases: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
