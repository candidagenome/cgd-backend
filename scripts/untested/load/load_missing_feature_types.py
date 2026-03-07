#!/usr/bin/env python3
"""
Load missing feature types into the database.

This script identifies features that don't have entries in the FEATURE_TYPE
table and creates appropriate entries for them based on their feature names.

Features with names starting with "orf" are assigned the "ORF" feature type.

Original Perl: loadMissingFeatureTypes.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal

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


def get_features_without_type(session: Session) -> list[tuple]:
    """
    Get features that don't have an entry in FEATURE_TYPE table.

    Args:
        session: Database session

    Returns:
        List of (feature_no, feature_name) tuples
    """
    # Use raw SQL to find features without feature_type entries
    query = text("""
        SELECT feature_no, feature_name
        FROM feature
        WHERE feature_no NOT IN (
            SELECT feature_no FROM feature_type
        )
    """)

    result = session.execute(query)
    features = [(row[0], row[1]) for row in result]

    logger.info(f"Found {len(features)} features without feature types")
    return features


def determine_feature_type(feature_name: str) -> str | None:
    """
    Determine feature type based on feature name pattern.

    Args:
        feature_name: Name of the feature

    Returns:
        Feature type string or None if cannot determine
    """
    if feature_name.lower().startswith("orf"):
        return "ORF"

    # Add more patterns here as needed
    # e.g., tRNA, rRNA, etc.

    return None


def insert_feature_type(
    session: Session,
    feature_no: int,
    feature_type: str,
    created_by: str,
) -> bool:
    """
    Insert a new feature_type entry.

    Args:
        session: Database session
        feature_no: Feature number
        feature_type: Type of feature
        created_by: User creating the record

    Returns:
        True if inserted successfully
    """
    query = text("""
        INSERT INTO feature_type (feature_no, feature_type, date_created, created_by)
        VALUES (:feature_no, :feature_type, CURRENT_TIMESTAMP, :created_by)
    """)

    session.execute(query, {
        "feature_no": feature_no,
        "feature_type": feature_type,
        "created_by": created_by[:12],
    })

    return True


def load_missing_feature_types(
    session: Session,
    created_by: str,
) -> dict:
    """
    Load missing feature types into the database.

    Args:
        session: Database session
        created_by: User creating the records

    Returns:
        Dictionary with statistics
    """
    stats = {
        "features_checked": 0,
        "types_inserted": 0,
        "types_undetermined": 0,
        "undetermined_features": [],
    }

    features = get_features_without_type(session)
    stats["features_checked"] = len(features)

    for feature_no, feature_name in features:
        feature_type = determine_feature_type(feature_name)

        if feature_type:
            insert_feature_type(session, feature_no, feature_type, created_by)
            stats["types_inserted"] += 1
            logger.info(f"Inserted feature type '{feature_type}' for {feature_name}")
        else:
            stats["types_undetermined"] += 1
            stats["undetermined_features"].append(feature_name)
            logger.warning(
                f"Unable to determine feature type for: {feature_name}"
            )

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load missing feature types into the database"
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
        help="Check for missing types but don't modify database",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_file, args.verbose)

    logger.info(f"Created by: {args.created_by}")

    try:
        with SessionLocal() as session:
            if args.dry_run:
                logger.info("DRY RUN - no database modifications")
                features = get_features_without_type(session)

                orfs = [f for f in features if f[1].lower().startswith("orf")]
                others = [f for f in features if not f[1].lower().startswith("orf")]

                logger.info(f"Would insert {len(orfs)} ORF feature types")
                if others:
                    logger.warning(f"Cannot determine type for {len(others)} features:")
                    for feature_no, feature_name in others[:10]:  # Show first 10
                        logger.warning(f"  - {feature_name}")
                    if len(others) > 10:
                        logger.warning(f"  ... and {len(others) - 10} more")
                return

            stats = load_missing_feature_types(session, args.created_by)

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Features checked: {stats['features_checked']}")
            logger.info(f"  Feature types inserted: {stats['types_inserted']}")
            if stats["types_undetermined"] > 0:
                logger.warning(
                    f"  Could not determine type for: {stats['types_undetermined']}"
                )
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading feature types: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
