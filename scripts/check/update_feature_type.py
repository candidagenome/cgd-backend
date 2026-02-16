#!/usr/bin/env python3
"""
Update feature_type for a list of features.

This script updates the feature_type field for features listed in an
input file. It can change features from one type to another.

Input file format: One ORF name per line (orf\\d+.\\d+.\\d* pattern extracted)

Original Perl: updateFeature_type.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import re
import sys
from datetime import datetime
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


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def find_feature_by_name(session: Session, name: str) -> Feature | None:
    """
    Find feature by name or alias.

    Args:
        session: Database session
        name: Feature name or alias

    Returns:
        Feature object or None
    """
    # Try feature_name first
    feature = session.query(Feature).filter(
        Feature.feature_name == name
    ).first()

    if feature:
        return feature

    # Try as alias
    alias = session.query(Alias).filter(
        Alias.alias_name == name
    ).first()

    if alias:
        feat_alias = session.query(FeatAlias).filter(
            FeatAlias.alias_no == alias.alias_no
        ).first()

        if feat_alias:
            return session.query(Feature).filter(
                Feature.feature_no == feat_alias.feature_no
            ).first()

    return None


def extract_orf_names(filepath: Path) -> list[str]:
    """
    Extract ORF names from input file.

    Args:
        filepath: Path to input file

    Returns:
        List of ORF names
    """
    orfs = []
    pattern = re.compile(r'(orf\d+\.\d+\.?\d*)', re.IGNORECASE)

    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            match = pattern.search(line)
            if match:
                orfs.append(match.group(1))

    return orfs


def update_feature_types(
    session: Session,
    orf_names: list[str],
    old_type: str,
    new_type: str,
) -> dict:
    """
    Update feature types for given ORFs.

    Args:
        session: Database session
        orf_names: List of ORF names
        old_type: Current feature type (for verification)
        new_type: New feature type

    Returns:
        Statistics dict
    """
    stats = {
        "total": len(orf_names),
        "updated": 0,
        "not_found": 0,
        "wrong_type": 0,
        "already_correct": 0,
    }

    for orf_name in orf_names:
        feature = find_feature_by_name(session, orf_name)

        if not feature:
            logger.warning(f"Feature not found: {orf_name}")
            stats["not_found"] += 1
            continue

        # Check current type
        if feature.feature_type == new_type:
            logger.debug(f"{orf_name}: Already has type '{new_type}'")
            stats["already_correct"] += 1
            continue

        if old_type and feature.feature_type != old_type:
            logger.warning(
                f"{orf_name}: Expected type '{old_type}', found '{feature.feature_type}'"
            )
            stats["wrong_type"] += 1
            continue

        # Update type
        old = feature.feature_type
        feature.feature_type = new_type
        logger.info(f"{orf_name}: Updated type '{old}' -> '{new_type}'")
        stats["updated"] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update feature_type for a list of features"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input file with ORF names",
    )
    parser.add_argument(
        "--old-type",
        help="Expected current feature type (for verification)",
    )
    parser.add_argument(
        "--new-type",
        required=True,
        help="New feature type to set",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without modifying database",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    logger.info(f"Started at {datetime.now()}")

    # Validate input file
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    # Extract ORF names
    orf_names = extract_orf_names(args.input_file)
    logger.info(f"Found {len(orf_names)} ORF names in input file")

    if not orf_names:
        logger.warning("No ORF names found")
        return

    logger.info(f"Old type: {args.old_type or '(any)'}")
    logger.info(f"New type: {args.new_type}")

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")

    try:
        with SessionLocal() as session:
            stats = update_feature_types(
                session,
                orf_names,
                args.old_type,
                args.new_type,
            )

            if not args.dry_run:
                session.commit()
                logger.info("Transaction committed")
            else:
                session.rollback()
                logger.info("Transaction rolled back (dry run)")

            logger.info("=" * 50)
            logger.info("Update Summary:")
            logger.info(f"  Total ORFs: {stats['total']}")
            logger.info(f"  Updated: {stats['updated']}")
            logger.info(f"  Not found: {stats['not_found']}")
            if args.old_type:
                logger.info(f"  Wrong type: {stats['wrong_type']}")
            logger.info(f"  Already correct: {stats['already_correct']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
