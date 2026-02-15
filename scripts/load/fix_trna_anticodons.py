#!/usr/bin/env python3
"""
Fix tRNA feature names by converting T to U in anticodons.

This script finds tRNA features with names containing DNA anticodon notation
(using T for thymine) and updates them to use RNA notation (U for uracil).

For example: tA(TGC)1 -> tA(UGC)1

Original Perl: fixTRNAs.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Feature

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


def get_trna_features_with_dna_anticodons(session: Session) -> list[tuple]:
    """
    Find tRNA features with DNA anticodon notation (containing T).

    Args:
        session: Database session

    Returns:
        List of (feature_no, feature_name) tuples
    """
    # Find features with pattern like t%(%)% containing T in the anticodon
    features = session.query(Feature).filter(
        Feature.feature_name.like("t%(%)%")
    ).all()

    results = []
    for f in features:
        # Check if anticodon contains T
        match = re.search(r"\(([ACGT]{3})\)", f.feature_name)
        if match and "T" in match.group(1):
            results.append((f.feature_no, f.feature_name))

    logger.info(f"Found {len(results)} tRNA features with DNA anticodon notation")
    return results


def fix_anticodon(name: str) -> str:
    """
    Convert DNA anticodon to RNA notation (T -> U).

    Args:
        name: Feature name with DNA anticodon

    Returns:
        Feature name with RNA anticodon
    """
    match = re.search(r"\(([ACGT]{3})\)", name)
    if match:
        anticodon = match.group(1)
        rna_anticodon = anticodon.replace("T", "U")
        return name.replace(f"({anticodon})", f"({rna_anticodon})")
    return name


def fix_trna_anticodons(
    session: Session,
    updated_by: str,
) -> dict:
    """
    Fix tRNA feature names to use RNA anticodon notation.

    Args:
        session: Database session
        updated_by: User making the update

    Returns:
        Dictionary with statistics
    """
    stats = {
        "features_found": 0,
        "features_updated": 0,
        "changes": [],
    }

    features = get_trna_features_with_dna_anticodons(session)
    stats["features_found"] = len(features)

    for feature_no, old_name in features:
        new_name = fix_anticodon(old_name)

        if old_name == new_name:
            continue

        # Update the feature name
        feature = session.query(Feature).filter(
            Feature.feature_no == feature_no
        ).first()

        if feature:
            feature.feature_name = new_name
            stats["features_updated"] += 1
            stats["changes"].append({
                "feature_no": feature_no,
                "old_name": old_name,
                "new_name": new_name,
            })
            logger.info(f"{old_name} -> {new_name}")

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Fix tRNA feature names (T -> U in anticodons)"
    )
    parser.add_argument(
        "--updated-by",
        default=os.getenv("DB_USER", "SCRIPT"),
        help="Database user name for audit",
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
        help="Find features but don't modify database",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_file, args.verbose)

    logger.info(f"Updated by: {args.updated_by}")

    try:
        with SessionLocal() as session:
            if args.dry_run:
                logger.info("DRY RUN - no database modifications")
                features = get_trna_features_with_dna_anticodons(session)
                for feature_no, name in features:
                    new_name = fix_anticodon(name)
                    logger.info(f"Would update: {name} -> {new_name}")
                logger.info(f"Would update {len(features)} features")
                return

            stats = fix_trna_anticodons(session, args.updated_by)

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Update Summary:")
            logger.info(f"  Features found: {stats['features_found']}")
            logger.info(f"  Features updated: {stats['features_updated']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error fixing tRNA anticodons: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
